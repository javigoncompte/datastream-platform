"""
This type stub file was generated by pyright.
"""

"""Recognize image file formats based on their first few bytes."""
__all__ = ["what"]
def what(file, h=...): # -> None:
    ...

tests = ...
def test_jpeg(h, f): # -> Literal['jpeg'] | None:
    """JPEG data with JFIF or Exif markers; and raw JPEG"""
    ...

def test_png(h, f): # -> Literal['png'] | None:
    ...

def test_gif(h, f): # -> Literal['gif'] | None:
    """GIF ('87 and '89 variants)"""
    ...

def test_tiff(h, f): # -> Literal['tiff'] | None:
    """TIFF (can be in Motorola or Intel byte order)"""
    ...

def test_rgb(h, f): # -> Literal['rgb'] | None:
    """SGI image library"""
    ...

def test_pbm(h, f): # -> Literal['pbm'] | None:
    """PBM (portable bitmap)"""
    ...

def test_pgm(h, f): # -> Literal['pgm'] | None:
    """PGM (portable graymap)"""
    ...

def test_ppm(h, f): # -> Literal['ppm'] | None:
    """PPM (portable pixmap)"""
    ...

def test_rast(h, f): # -> Literal['rast'] | None:
    """Sun raster file"""
    ...

def test_xbm(h, f): # -> Literal['xbm'] | None:
    """X bitmap (X10 or X11)"""
    ...

def test_bmp(h, f): # -> Literal['bmp'] | None:
    ...

def test_webp(h, f): # -> Literal['webp'] | None:
    ...

def test_exr(h, f): # -> Literal['exr'] | None:
    ...

def test(): # -> None:
    ...

def testall(list, recursive, toplevel): # -> None:
    ...

if __name__ == '__main__':
    ...
