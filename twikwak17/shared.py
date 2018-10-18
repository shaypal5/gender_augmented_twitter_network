"""Shared stuff for twikwak17."""

from birch import Birch

TWIK_CFG = Birch('twikwak17')

DEF_FNAME_PATTERN = 'tweets2009-{:02d}.txt.gz'
DEF_FNAMES = [
    DEF_FNAME_PATTERN.format(month)
    for month in range(6, 13)
]
