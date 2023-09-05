"""Helper functions for 'merge fastq'
"""

import logging
import re
from typing import List, Set

from latch.functions.messages import message


logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO
)

def log(msg: str, title: str, type: str="info"):
    getattr(logging, type)(msg)
    message(typ=type, data={"title": title, "body": msg})

def test_reads(file_names: List[str]) -> Set[str]:
    """Extract read type (ie. R1, R2) and test for uniformity."""

    read_re = re.compile("_[R|I]?[1|2][_|.]") # ie. ('_R2_', '_1.', etc.) 
    replace_re = re.compile("[._R]") # strip read ID to number only
    read_ids = {re.sub(replace_re, "", re.search(read_re, name).group()) 
                for name in file_names
                if re.search(read_re, name)}
    
    if all(re.search(read_re, name) for name in file_names):
        
        if len(read_ids) > 1:
            msg = f"Multiple read types detected: {','.join(read_ids)}"
            log(msg, "read test", "warning")

    else:
        log(
            "One or more files missing read ID (ie. R1, R2)",
            "read test",
            "warning"
        )
    
    return read_ids

def test_extensions(file_names: List[str]) -> Set[str]:
    """Extract allowed fastq file extensions and test for uninformity"""

    extension_re = re.compile("\.fastq\.gz$|.fq.gz$|\.fastq$|\.gz$|\.fq$")
    extensions = {re.search(extension_re, name).group()
                  for name in file_names
                  if re.search(extension_re, name)}

    allowed_extensions = [{'.fq', '.fastq'}, {'.fq.gz', '.fastq.gz'}]
    if len(extensions) > 1:
            if extensions in allowed_extensions:
                log(
                    "'.fq' and '.fastq' detected; returning '.fastq'",
                    "extension info"
                )
                return extensions.pop()
            
            elif extensions not in allowed_extensions:
               log(
                    "Multiple extensions detected; returning '.txt'",
                    "extension warning",
                    "warning"
                    ) 
               return ".txt"
            
    elif len(extensions) == 0:
        log(
            "No file extensions detected, please check inputs.",
            "extension warning",
            "warning"
        )

    return extensions

