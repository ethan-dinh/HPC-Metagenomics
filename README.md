# Shotgun Metagenomics Processing Pipeline
This pipeline is designed to process shotgun metagenomic data on an SGE high performance compute cluster such as UCSF Wynton. It utilizes, Kneaddata, Kraken2, and Bracken to compute taxonomic abundances. In addition, it also performs HUMAnN3 to compute functional abundances via MetaPhlAn3.

## Setup
1. Clone the repository into the root directory of your home directory on the SGE cluster.

```bash
git clone https://github.com/ethan-dinh/metagenomics.git
```

**TODO**: Finish the tutorial for running the pipeline. This includes the setup, single sample runs (on dev node + queue for the compute nodes), array job queueing, and troubleshooting. 
* Include a section on how to make the manifest file. This is a CSV file with the following columns: sample_id,fastq_1,fastq_2. The sample_id is the name of the sample. The fastq_1 and fastq_2 are the paths to the fastq files. The fastq files should be in the data directory. The manifest file should be in the data directory.

**TODO**: Write a section explaining the flags and how to use them. 

**TODO**: Write a section explaining the UCSF Wynton specific things such as the ssh key and the DT node. Also explain the scratch space and how to use it. Also explain how limited the HOME directory storage is. 
* Include a section on this specific error: 

``` bash
can't get password entry for user "USER". Either user does not exist or error with NIS/LDAP etc.
```

To determine which node is failing, run:
``` bash
tail -100000 /opt/sge/wynton/common/accounting | awk --field-separator=':' '$12 == 1 { print $2 }' | sort | uniq -c | sort -r -h -k 1
```

You can determine which node is failing and you can queue a job that is not on that node by:
``` bash
qsub -l hostname="\!qb3-id138" myscript.sh
```

**Include**: A template script that automatically queues the jobs for the compute nodes. This script should call the main script with the appropriate flags and provide the manifest file as an argument.

**TODO**: Write a section on how to install the dependencies via the init.sh script.

**TODO**: Also write a section on how the transfer to box works. Include the util files. Write the script that allows the user to automatically create the ssh key so that the main script can call the DT node to transfer the output files to Box. 
* Explain the setup of the Box account and user specific password. Since the sign in is SSO, the user needs to create a user specific password. Explain how to create the .netrc file based on the instructions from Box.

**TODO**: Include information on why the script is limited to 25 concurrent transfer. This is due to the throttling on the Box FTP server. The compute could be done very fast, but if you want to successfully transfer the files as they finish then you need to limit the number of concurrent transfers. Otherwise, you can use a service like Globus to transfer the files afterwards. I find this clunky and slow, but it is also a good option if you just want to process the files quickly. Additionally, if you want to transfer the files faster, you can uncomment the removal of the fastq files. This will delete the fastq files after all the processing is done. I do not recommend this as it is a waste of the time to re-run kneaddata. 

**TODO**: Switch to relative paths for the helper scripts so that they can be run from any directory. 

**TODO**: Include scripts for downloading the databases for Kneaddata, Bracken, Kraken2, and MetaPhlAn3. This is a database section. 

## Future Work: Pipeline Monitoring Tool

A key area for future development is adding a monitoring service to track the progress of the pipeline. Monitoring should provide visibility into job status (`qstat`), log updates, and output files all in real time and in one place. This will allow the user to continuously monitor their pipeline without having to check different logs and output files. 

### Proposed Features

1. **Terminal User Interface (TUI):**
   * Display all samples currently running.
   * Show the step each sample is on, based on step-echoing added to `run_metagenomics.sh`.
   * Allow navigation between an overview screen and detailed job views.
   * When a user selects a specific job, the tool will open its logs and update them in real time.

2. **Integration with Job Scheduler:**
   * Parse the output of `qstat` to determine which jobs are currently running.
   * Match task IDs against the manifest to identify which samples are in progress.

3. **Progress Tracking via Logs:**
   * Monitor log files to determine completion status.
   * Logic:
     * If a log file exists and includes sample completion markers, the job is finished.
     * If a log file is missing or incomplete, the job is still running or has not started.

4. **Background Monitoring:**
   * A lightweight process will run alongside the pipeline, updating the TUI as jobs advance through steps.

### Implementation Considerations

* **Language:**
  * A Bash script is sufficient for a basic prototype (parsing `qstat` and tailing logs).
  * A C implementation with `ncurses` would provide a polished, interactive TUI similar to `htop`.

* **Distribution:**
  * The recommended approach is to write the tool in C, package it with the pipeline, and provide both:
    * A precompiled binary for convenience.
    * The source code and Makefile for users who wish to build it themselves.
