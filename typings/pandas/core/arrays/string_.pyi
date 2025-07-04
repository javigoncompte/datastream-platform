"""
This type stub file was generated by pyright.
"""

import numpy as np
import pyarrow
from typing import ClassVar, Literal, TYPE_CHECKING
from pandas._libs import missing as libmissing
from pandas.util._decorators import doc
from pandas.core.dtypes.base import StorageExtensionDtype, register_extension_dtype
from pandas.core.arrays.base import ExtensionArray
from pandas.core.arrays.numpy_ import NumpyExtensionArray
from pandas._typing import NumpySorter, NumpyValueArrayLike, Scalar, Self, npt, type_t
from pandas import Series

if TYPE_CHECKING:
    ...
@register_extension_dtype
class StringDtype(StorageExtensionDtype):
    """
    Extension dtype for string data.

    .. warning::

       StringDtype is considered experimental. The implementation and
       parts of the API may change without warning.

    Parameters
    ----------
    storage : {"python", "pyarrow", "pyarrow_numpy"}, optional
        If not given, the value of ``pd.options.mode.string_storage``.

    Attributes
    ----------
    None

    Methods
    -------
    None

    Examples
    --------
    >>> pd.StringDtype()
    string[python]

    >>> pd.StringDtype(storage="pyarrow")
    string[pyarrow]
    """
    name: ClassVar[str] = ...
    @property
    def na_value(self) -> libmissing.NAType | float:
        ...
    
    _metadata = ...
    def __init__(self, storage=...) -> None:
        ...
    
    @property
    def type(self) -> type[str]:
        ...
    
    @classmethod
    def construct_from_string(cls, string) -> Self:
        """
        Construct a StringDtype from a string.

        Parameters
        ----------
        string : str
            The type of the name. The storage type will be taking from `string`.
            Valid options and their storage types are

            ========================== ==============================================
            string                     result storage
            ========================== ==============================================
            ``'string'``               pd.options.mode.string_storage, default python
            ``'string[python]'``       python
            ``'string[pyarrow]'``      pyarrow
            ========================== ==============================================

        Returns
        -------
        StringDtype

        Raise
        -----
        TypeError
            If the string is not a valid option.
        """
        ...
    
    def construct_array_type(self) -> type_t[BaseStringArray]:
        """
        Return the array type associated with this dtype.

        Returns
        -------
        type
        """
        ...
    
    def __from_arrow__(self, array: pyarrow.Array | pyarrow.ChunkedArray) -> BaseStringArray:
        """
        Construct StringArray from pyarrow Array/ChunkedArray.
        """
        ...
    


class BaseStringArray(ExtensionArray):
    """
    Mixin class for StringArray, ArrowStringArray.
    """
    @doc(ExtensionArray.tolist)
    def tolist(self): # -> list[Any]:
        ...
    


class StringArray(BaseStringArray, NumpyExtensionArray):
    """
    Extension array for string data.

    .. warning::

       StringArray is considered experimental. The implementation and
       parts of the API may change without warning.

    Parameters
    ----------
    values : array-like
        The array of data.

        .. warning::

           Currently, this expects an object-dtype ndarray
           where the elements are Python strings
           or nan-likes (``None``, ``np.nan``, ``NA``).
           This may change without warning in the future. Use
           :meth:`pandas.array` with ``dtype="string"`` for a stable way of
           creating a `StringArray` from any sequence.

        .. versionchanged:: 1.5.0

           StringArray now accepts array-likes containing
           nan-likes(``None``, ``np.nan``) for the ``values`` parameter
           in addition to strings and :attr:`pandas.NA`

    copy : bool, default False
        Whether to copy the array of data.

    Attributes
    ----------
    None

    Methods
    -------
    None

    See Also
    --------
    :func:`pandas.array`
        The recommended function for creating a StringArray.
    Series.str
        The string methods are available on Series backed by
        a StringArray.

    Notes
    -----
    StringArray returns a BooleanArray for comparison methods.

    Examples
    --------
    >>> pd.array(['This is', 'some text', None, 'data.'], dtype="string")
    <StringArray>
    ['This is', 'some text', <NA>, 'data.']
    Length: 4, dtype: string

    Unlike arrays instantiated with ``dtype="object"``, ``StringArray``
    will convert the values to strings.

    >>> pd.array(['1', 1], dtype="object")
    <NumpyExtensionArray>
    ['1', 1]
    Length: 2, dtype: object
    >>> pd.array(['1', 1], dtype="string")
    <StringArray>
    ['1', '1']
    Length: 2, dtype: string

    However, instantiating StringArrays directly with non-strings will raise an error.

    For comparison methods, `StringArray` returns a :class:`pandas.BooleanArray`:

    >>> pd.array(["a", None, "c"], dtype="string") == "a"
    <BooleanArray>
    [True, <NA>, False]
    Length: 3, dtype: boolean
    """
    _typ = ...
    def __init__(self, values, copy: bool = ...) -> None:
        ...
    
    def __arrow_array__(self, type=...):
        """
        Convert myself into a pyarrow Array.
        """
        ...
    
    def __setitem__(self, key, value) -> None:
        ...
    
    def astype(self, dtype, copy: bool = ...): # -> Self | IntegerArray | FloatingArray | ExtensionArray | NDArray[Any] | ndarray[Any, Any]:
        ...
    
    def min(self, axis=..., skipna: bool = ..., **kwargs) -> Scalar:
        ...
    
    def max(self, axis=..., skipna: bool = ..., **kwargs) -> Scalar:
        ...
    
    def value_counts(self, dropna: bool = ...) -> Series:
        ...
    
    def memory_usage(self, deep: bool = ...) -> int:
        ...
    
    @doc(ExtensionArray.searchsorted)
    def searchsorted(self, value: NumpyValueArrayLike | ExtensionArray, side: Literal["left", "right"] = ..., sorter: NumpySorter | None = ...) -> npt.NDArray[np.intp] | np.intp:
        ...
    
    _arith_method = ...
    _str_na_value = ...


