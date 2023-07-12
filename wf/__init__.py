"""Latch.bio workflow for merging fastq files
"""

import subprocess

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

@medium_task
def merge_task(input_files: List[LatchFile], output_dir: str) -> LatchFile:

    input_files = [file.local_path for file in input_files]
    out_file = f"merged_r1.fastq.gz"

    _merge_cmd = ["cat"] + [input_files]
    subprocess.run(_merge_cmd, stdout=open(out_file, "w"))

    local_location = f"/root/{out_file}"
    remote_location = f"latch:///merged/{output_dir}/{out_file}"

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
                )
            ]
        ),
    },
)

@workflow(metadata)
def template_workflow(
    input_files: List[LatchFile], output_dir: str
) -> LatchFile:
    return merge_task(input_files=input_files, output_dir=output_dir)
