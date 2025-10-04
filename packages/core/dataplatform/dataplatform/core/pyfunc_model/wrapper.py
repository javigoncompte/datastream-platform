from collections.abc import Callable, Generator
from typing import Any, Dict, List, Optional, Union

import numpy
import polars as pl
import pyspark.pandas as ps
from mlflow.pyfunc.model import PythonModel, PythonModelContext
from pyspark.sql import DataFrame as SparkDataFrame
from dataplatform.core.logger import get_logger

MlDataType = Union[
    pl.DataFrame,
    pl.Series,
    numpy.ndarray,
    List,
    Dict,
    SparkDataFrame,
    ps.DataFrame,
    ps.Series,
]

logger = get_logger(__name__)


class Model(PythonModel):
    def __init__(
        self,
        model: Callable,
        preprocess_input_fn: Callable | None = None,
        postprocess_output_fn: Callable | None = None,
    ):
        self.model = model
        self.preprocess_input_fn = preprocess_input_fn
        self.postprocess_output_fn = postprocess_output_fn

    def preprocess_input(self, model_input: Any) -> MlDataType:
        if self.preprocess_input_fn:
            return self.preprocess_input_fn(model_input)
        return model_input

    def postprocess_output(self, model_output: Any) -> MlDataType:
        if self.postprocess_output_fn:
            return self.postprocess_output_fn(model_output)
        return model_output

    def predict(self, context, model_input: Any, params=None) -> MlDataType:
        params = params or {"predict_method": "predict"}
        predict_method = params.get("predict_method")
        model_input = self.preprocess_input(model_input)
        if predict_method == "predict":
            outputs = self.model.predict(model_input)
        elif predict_method == "predict_proba":
            outputs = self.model.predict_proba(model_input)
        else:
            raise ValueError(f"Invalid predict method: {predict_method}")
        outputs = self.postprocess_output(outputs)
        return outputs

    def predict_stream(
        self,
        context: PythonModelContext,
        model_input: Any,
        params: Optional[dict[str, Any]] = None,
    ) -> Generator[MlDataType, None, None]:
        if self.preprocess_input_fn:
            model_input = self.preprocess_input(model_input)
        outputs = self.model.predict_stream(model_input)
        for output in outputs:
            if self.postprocess_output_fn:
                yield self.postprocess_output_fn(output)
            else:
                yield output


def create_model(
    model: Callable,
    preprocess_input_fn: Callable | None = None,
    postprocess_output_fn: Callable | None = None,
) -> Model:
    return Model(
        model=model,
        preprocess_input_fn=preprocess_input_fn,
        postprocess_output_fn=postprocess_output_fn,
    )
