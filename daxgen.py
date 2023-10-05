#!/usr/bin/env python3
from typing import Optional, Dict, List
from argparse import ArgumentParser
import sys
import logging
import os
import csv
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)

# --- Import Pegasus API ------------------------------------------------------

# API Documentation: http://pegasus.isi.edu/documentation

class GenomeWorkflow(object):
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self,
                    datafile: str = 'data.csv',
                    dataset: str = '20130502',
                    ind_jobs: int = 250,
                    exec_site: Optional[str] = "condorpool",
                    use_bash: Optional[bool] = False,
                    src_path: Optional[str] = None,
                    columns: str = 'columns.txt',
                    use_decaf: Optional[bool] = False,
                    use_pmc: Optional[bool] = False,
                    custom_site_file: Optional[str] = None,
                ) -> None:

        self.wf_name = "1000-genome"
        self.wid = self.wf_name + "-" + datetime.now().strftime("%s")
        self.dagfile = self.wid+".yml"
        self.wf_dir = str(Path(__file__).parent.resolve()) + '/'
        self.src_path = self.wf_dir
        if src_path:
            self.src_path = src_path
        
        self.dataset = dataset
        self.datafile = datafile
        self.exec_site = exec_site
        self.columns = File(columns)
        self.ind_jobs = ind_jobs
        self.use_decaf = use_decaf
        self.use_pmc = use_pmc
        self.custom_site_file = custom_site_file

        if self.use_decaf:
            print("Using Decaf...")
            
        if self.use_pmc:
            print("Using PMC...")
        
        self.file_site = "local"
        if self.exec_site == "cori":
            self.file_site = "cori"
        
        self.suffix = ".py"
        if use_bash:
            self.suffix = ""

        ## Output Sites
        self.shared_scratch_dir = os.path.join(
            self.wf_dir, "{}/scratch".format(self.wid))
        self.local_storage_dir = os.path.join(
            self.wf_dir, "{}/output".format(self.wid))


        # Population Files
        self.populations = []
        for base_file in os.listdir(self.wf_dir + 'data/populations/'):
          f_pop = File(base_file)
          self.populations.append(f_pop)
        #   break


    # --- Write files in directory --------------------------------------------
    def write(self, produce_dot: bool = False):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.tc.write()
        self.rc.write()
        self.wf.write()




    # --- Site Catalog --------------------------------------------------------
    def create_sites_catalog(self) -> None:
        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, self.local_storage_dir).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        condorpool = (
            Site("condorpool")
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, condorpool)

    # --- Transformation Catalog (Executables and Containers) -----------------

    def create_transformation_catalog(self) -> None:
        self.tc = TransformationCatalog()
        
        # set of tasks, individuals. Take an input of produce an output of chr{}n-{}-{}.tar.gz

        e_individuals = (
            Transformation(
                "individuals",
                site="local",
                pfn=self.src_path + '/bin/individuals' + self.suffix,
                is_stageable=True,
            )
            # .add_profiles(Namespace.PEGASUS, key="label", value="decaf")
        )
        e_individuals_merge = (
            Transformation(
                "individuals_merge",
                site="local",
                pfn=self.src_path + '/bin/individuals_merge' + self.suffix,
                is_stageable=True,
            )
            # .add_profiles(Namespace.PEGASUS, key="label", value="decaf")
        )
        e_sifting = Transformation(
            "sifting",
            site="local",
            pfn=self.src_path+ '/bin/sifting' + self.suffix,
            is_stageable=True,
        )
        e_mutation_overlap = Transformation(
            "mutation_overlap",
            site="local",
            pfn=self.src_path + '/bin/mutation_overlap' + self.suffix,
            is_stageable=True,
        )
        e_freq = Transformation(
            "frequency",
            site="local",
            pfn=self.src_path + '/bin/frequency.py',
            is_stageable=True,
        )

        self.tc.add_transformations(
            e_individuals, e_individuals_merge, e_sifting, e_mutation_overlap, e_freq)

    # --- Replica Catalog -----------------

    def create_replica_catalog(self) -> None:
        self.rc = ReplicaCatalog()

        # declare all data files, data/20..../

        self.rc.add_replica(site=self.file_site, lfn=self.columns,
                            pfn=self.src_path + '/data/' + self.dataset + '/' + self.columns.lfn)

        for popfile in self.populations:
            self.rc.add_replica(site=self.file_site, lfn=popfile,
                                pfn=self.src_path + '/data/populations/' + popfile.lfn)

    # --- Create Workflow -----------------------------------------------------

    def create_workflow(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        c_nums = []
        individuals_files = []
        sifted_files = []
        sifted_jobs = []
        individuals_merge_jobs = []
        
        with open(self.datafile, 'r') as f:
            for row in csv.reader(f):
                # base file, 250k, annotations
                base_file = row[0]
                threshold = int(row[1])
                # To ensure we do not create too many individuals jobs
                ind_jobs = min(self.ind_jobs, threshold)
                step = threshold // ind_jobs
                rest = threshold % ind_jobs
                if rest != 0:
                    sys.exit("ERROR: for file {}: required individuals jobs {} does not divide the number of rows {}.".format(
                        base_file, ind_jobs, threshold))

                counter = 1

                individuals_jobs = []
                output_files = []

                # Individuals Jobs
                # the input file for individuals is the base file 
                f_individuals = File(base_file)
                self.rc.add_replica(site=self.file_site, lfn=f_individuals, pfn=self.src_path +
                                    '/data/' + self.dataset + '/' + f_individuals.lfn)

                # get the c number (chromosome?). Looks like it is in the filename eg ALL.chr1.250000.vcf 
                c_num = base_file[base_file.find('chr')+3:]
                c_num = c_num[0:c_num.find('.')]
                c_nums.append(c_num)

                # while the counter is less than num rows of data file
                # step / threshold num jobs
                while counter < threshold:
                    stop = counter + step

                    # we create an ouput file of the format, 
                    out_name = 'chr%sn-%s-%s.tar.gz' % (c_num, counter, stop)
                    output_files.append(out_name)
                    # f chr new output file
                    f_chrn = File(out_name)


                    # new job with args - data_file, chr number, number of chr? number of proc?, max proc?
                    # ./individuals ALL.chr1.xyz 1 100 200 250000 
                    # input pegasus file objects, f_indiv, columns.txt?
                    # output pegasus file objects chrn. some flags
                    j_individuals = (
                        Job('individuals')
                            .add_args(f_individuals, c_num, str(counter), str(stop), str(threshold))
                            .add_inputs(f_individuals, self.columns)
                            .add_outputs(f_chrn, stage_out=False, register_replica=False)
                    )
                    if self.use_decaf or self.use_pmc:
                        j_individuals.add_profiles(Namespace.PEGASUS, key="label", value="cluster1")

                    individuals_jobs.append(j_individuals)
                    self.wf.add_jobs(j_individuals)

                    counter = counter + step

                # merge job
                j_individuals_merge = Job('individuals_merge').add_args(c_num)

                for out_name in output_files:
                    f_chrn = File(out_name)
                    j_individuals_merge.add_inputs(f_chrn)
                    j_individuals_merge.add_args(f_chrn)

                individuals_filename = 'chr%sn.tar.gz' % c_num
                f_chrn_merged = File(individuals_filename)
                individuals_files.append(f_chrn_merged)
                j_individuals_merge.add_outputs(f_chrn_merged, stage_out=False, register_replica=False)
                if self.use_decaf or self.use_pmc:
                    j_individuals_merge.add_profiles(Namespace.PEGASUS, key="label", value="cluster1")

                self.wf.add_jobs(j_individuals_merge)
                individuals_merge_jobs.append(j_individuals_merge)
                
                # Sifting Job
                f_sifting = File(row[2])
                self.rc.add_replica(site=self.file_site, lfn=f_sifting, pfn=self.src_path +
                                    '/data/' + self.dataset + '/sifting/' + f_sifting.lfn)

                f_sifted = File('sifted.SIFT.chr%s.txt' % c_num)
                sifted_files.append(f_sifted)

                j_sifting = (
                    Job('sifting')
                        .add_inputs(f_sifting)
                        .add_outputs(f_sifted, stage_out=False, register_replica=False)
                        .add_args(f_sifting, c_num)
                )

                self.wf.add_jobs(j_sifting)
                sifted_jobs.append(j_sifting)

        # Analyses jobs
        for i in range(len(individuals_files)):
            for f_pop in self.populations:
                # Mutation Overlap Job
                f_mut_out = File('chr%s-%s.tar.gz' % (c_nums[i], f_pop.lfn))
                j_mutation = (
                    Job('mutation_overlap')
                        .add_args('-c', c_nums[i], '-pop', f_pop)
                        .add_inputs(individuals_files[i], sifted_files[i], f_pop, self.columns)
                        .add_outputs(f_mut_out, stage_out=True, register_replica=False)
                )
                # Frequency Mutations Overlap Job
                f_freq_out = File('chr%s-%s-freq.tar.gz' % (c_nums[i], f_pop.lfn))
                j_freq = (
                    Job('frequency')
                        .add_args('-c', c_nums[i], '-pop', f_pop)
                        .add_inputs(individuals_files[i], sifted_files[i], f_pop, self.columns)
                        .add_outputs(f_freq_out, stage_out=True, register_replica=False)
                )
                self.wf.add_jobs(j_mutation, j_freq)

    # --- Run Workflow -----------------------------------------------------

    def run(self, dir_name, submit=False, wait=False):
        try:
            plan_site = [self.exec_site]
            cluster_type = None
            if self.use_decaf or self.use_pmc: 
                cluster_type = ["label"]
            self.wf.plan(
                dir=self.wf_dir,
                relative_dir=dir_name,
                sites=plan_site,
                output_sites=["local"],
                output_dir=self.local_storage_dir,
                cleanup="leaf",
                force=True,
                submit=submit,
                cluster=cluster_type
                # verbose=3
            )
            if wait:
                self.wf.wait()

        except Exception as e:
            print(e)
            sys.exit(-1)


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus 1000Genome Workflow")

    parser.add_argument(
        "-s",
        "--submit",
        action="store_true",
        help="Submit the workflow",
    )
    parser.add_argument(
        "-e",
        "--execution-site",
        metavar="STR",
        type=str,
        default="local",
        help="Execution site name (default: local)",
    )
    parser.add_argument(
        '-D', 
        '--dataset', 
        action='store', 
        dest='dataset', 
        default='20130502', 
        help='Dataset folder'
    )
    parser.add_argument(
        '-f', 
        '--datafile', 
        action='store', 
        dest='datafile', 
        default='data.csv', 
        help='Data file with list of input data'
    )
    parser.add_argument(
        '-b', 
        '--bash-jobs', 
        action='store_true', 
        dest='use_bash', 
        help='Use original bash scripts for individuals, individuals_merge and sifting'
    )
    parser.add_argument(
        '-i',
        '--individuals-jobs',
        action='store',
        dest='ind_jobs',
        default=1,
        type=int,
        help='Number of individuals jobs that will be created for each chromosome \
            (if larger than the total number of rows in the data for that chromosome, \
            then it will be set to the number of rows so each job will process one row)'
    )
    parser.add_argument(
        "-p",
        "--src-path",
        metavar="STR",
        type=str,
        default=None,
        help="Absolute path of source directory you want to use. If this option is not specified, local current directory will be used",
    )
    parser.add_argument(
        '-d', 
        '--decaf', 
        action='store_true', 
        dest='use_decaf', 
        help='Use Decaf to stage data using memory'
    )
    parser.add_argument(
        '-c', 
        '--pmc', 
        action='store_true', 
        dest='use_pmc', 
        help='Use Pegasus MPI Cluster mode (PMC) (the compute nodes must have access to MPI)'
    )
    parser.add_argument(
        "-n",
        "--dir-name",
        metavar="STR",
        type=str,
        default=None,
        help="Name of the submit directory",
    )
    parser.add_argument(
        "-S",
        "--sites-catalog",
        metavar="STR",
        type=str,
        default=None,
        help="Use an existing site catalog (XML OR YAML)",
    )
    args = parser.parse_args()

    workflow = GenomeWorkflow(
        datafile = args.datafile,
        dataset = args.dataset,
        ind_jobs = args.ind_jobs,
        exec_site = args.execution_site,
        use_bash = args.use_bash,
        src_path = args.src_path,
        use_decaf = args.use_decaf,
        use_pmc = args.use_pmc,
        custom_site_file = args.sites_catalog
    )

    # catalog compute resources
    print("Creating execution sites...")
    workflow.create_sites_catalog()

    # catalog of executables to be run
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog()

    # input file catalog
    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    # create the workflow
    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    if not args.dir_name:
        args.dir_name = workflow.wid

    print("Workflow created in {}/{}".format(workflow.wf_dir, args.dir_name))

    workflow.run(args.dir_name, submit=args.submit, wait=False)

