import pandas as pd

def safe_left_join(left, right, on, validate, suffixes=("", "_right")):
    return left.merge(
        right, 
        on=on, 
      how="left", 
        validate=validate, 
        suffixes=suffixes
    )
