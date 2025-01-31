"""Latch.bio workflow for merging fastq files
"""

from flytekit.core.annotation import FlyteAnnotation
import subprocess
from typing import Annotated, List

from latch.resources.tasks import medium_task
from latch.resources.workflow import workflow
from latch.types.file import LatchFile
from latch.types.metadata import (
    LatchAuthor,
    LatchMetadata,
    LatchParameter,
    LatchRule
)
from latch.types.metadata import Params, Section, Text

from wf.utils import log, test_extensions, test_reads


@medium_task
def merge_task(
    run_id: str,
    input_files: List[LatchFile],
    output_dir: str
) -> LatchFile:

    input_files = [file.local_path for file in input_files]
    file_names = [file.split("/")[-1] for file in input_files]

    # extract read type and extensions, test for uniformity
    read_ids = test_reads(file_names)
    extensions = test_extensions(file_names)

    out_file = f"{run_id}_merged_R{''.join(read_ids)}{''.join(extensions)}"
    _merge_cmd = ["cat"] + input_files

    in_msg = f"Merging initiated with {' '.join(file_names)}"
    log(in_msg, "merge initiated")

    subprocess.run(_merge_cmd, stdout=open(out_file, "w"))

    log(f"Files successfully merged into {out_file}", "merge completed")

    local_location = f"/root/{out_file}"
    remote_location = f"latch://13502.account/merged/{output_dir}/{out_file}"

    log(f"Uploading files to {remote_location}", "upload")

    return LatchFile(str(local_location), remote_location)


flow = [
    Section(
        "Run Parameters",
        Text(
            "> IMPORTANT: the order of input files MUST be the same for "
            "read 1 and read 2.  For example, if the order of input files for "
            "the read 1 Execution is ['novogene_R1.fastq', 'basespace_R1.fastq"
            "'], for read 2 the order must be ['novogene_R2.fastq', "
            "'basespace_R2.fastq'].  If you have questions, please email "
            "support@atlasxomics.com."
        ),
        Params("run_id"),
        Params("input_files"),
        Params("output_dir")
    )
]

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
    flow=flow
)


@workflow(metadata)
def merge_workflow(
    run_id: str,
    input_files: List[Annotated[LatchFile, FlyteAnnotation({
        "rules": [{
            "regex": "\.fastq\.gz|.fq.gz\|\.fastq|\.fq",
            "message": "Only fastq files can be merged (.fastq.gz, .fq.gz,\
                        .fastq, .fq)"
                    }]
                })
            ]
        ],
    output_dir: str
) -> LatchFile:

    return merge_task(
        run_id=run_id,
        input_files=input_files,
        output_dir=output_dir
    )
