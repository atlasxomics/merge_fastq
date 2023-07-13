"""Latch.bio workflow for merging fastq files
"""

import logging
import subprocess
import re

from latch.functions.messages import message
from latch.resources.tasks import medium_task
from latch.resources.workflow import workflow
from latch.types.file import LatchFile
from latch.types.metadata import (
    LatchAuthor,
    LatchMetadata,
    LatchParameter,
    LatchRule
)
from typing import List

logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO
)

@medium_task
def merge_task(
    run_id: str, input_files: List[LatchFile], output_dir: str
) -> LatchFile:
        
    input_files = [file.local_path for file in input_files]
    file_names = [file.split("/")[-1] for file in input_files]

    # Check that all fastq files contain the same read
    read_re = "_[R|I][1|2]_" 
    read_ids = {re.search(read_re, name).group()
                for name in file_names
                if re.search(read_re, name)}
    len_reads = len(read_ids)
    if len_reads == 1:
        read_msg = f"Reads all of same type: {read_ids}" 
        logging.info(read_msg)
        message(typ="info", data={"body": read_msg})
    elif len_reads == 0:
        read_msg = "No read type (ie. R1) detected in file name" 
        logging.warning(read_msg)
        message(typ="warning", data={"body": read_msg})
    elif len_reads > 1:
        read_msg = f"Multiple read types detected: {read_ids}" 
        logging.warning(read_msg)
        message(typ="warning", data={"body": read_msg})

    # check that all fastq files have the same file prefix and type

    out_file = f"{run_id}_merged_r1.fastq.gz"

    _merge_cmd = ["cat"] + input_files

    in_msg = f"Merging initiated with {' '.join(file_names)}" 
    logging.info(in_msg)
    message(typ="info", data={"body": in_msg})

    subprocess.run(_merge_cmd, stdout=open(out_file, "w"))

    out_msg = f"Files successfully merged into {out_file}"
    logging.info(out_msg)
    message(typ="info", data={"body": out_msg})

    local_location = f"/root/{out_file}"
    remote_location = f"latch://13502.account/merged/{output_dir}/{out_file}"

    logging.info(f"Uploading files to {remote_location}")
    return LatchFile(str(local_location), remote_location)

metadata = LatchMetadata(
    display_name="merge fastq",
    author=LatchAuthor(
        name="AtlasXomics Inc.",
        email="jamesm@atlasxomics.com",
        github="https://github.com/atlasxomics",
    ),
    repository="https://github.com/atlasxomics/merge_fastq",
    license="MIT",
    parameters={
        "run_id": LatchParameter(
            display_name="run id",
            description="ATX Run ID with optional prefix, default to \
                        Dxxxxx_NGxxxxx format.",
            batch_table_column=True,
            placeholder="Dxxxxx_NGxxxxx",
            rules=[
                LatchRule(
                    regex="^[^/].*",
                    message="run id cannot start with a '/'"
                ),
                LatchRule(
                    regex="^\S+$",
                    message="run id cannot contain whitespace"
                )
            ]
        ),  
        "input_files": LatchParameter(
            display_name="input files",
            batch_table_column=True,
            description="list of fastq files to be merged",
        ),
        "output_dir": LatchParameter(
            display_name="output directory",
            batch_table_column=True,
            description="Name of Latch directory for merge fastq files; files \
                        will be saved to /merged/{output directory}.",
            rules=[
                LatchRule(
                    regex="^[^/].*",
                    message="output directory name cannot start with a '/'"
                ),
                LatchRule(
                    regex="^\S+$",
                    message="directory name cannot contain whitespace"
                )                
            ]
        ),
    },
)

@workflow(metadata)
def merge_workflow(
    run_id: str,
    input_files: List[LatchFile],
    output_dir: str
) -> LatchFile:
    
    return merge_task(
        run_id=run_id,
        input_files=input_files,
        output_dir=output_dir
    )