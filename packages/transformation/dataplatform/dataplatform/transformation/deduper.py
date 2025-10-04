from dataplatform.core.logger import get_logger


def dedupe_spark(
    df,
    subset=None,
    keep: str = "first",
    inplace: bool = False,
    ignore_index: bool = False,
):
    """
    Deduplicate a Spark DataFrame based on specified columns using the
    pandas API for Spark.

    :param df: The Spark DataFrame to deduplicate.
    :param subset: Column label or sequence of labels to consider for
                   deduplication. If None, use all columns.
    :param keep: {'first', 'last', False}, default 'first'. Determines which
                 duplicates to keep.
    :param inplace: If True, do operation inplace and return None.
    :param ignore_index: If True, reset index in the result.
    :return: A new DataFrame with duplicates removed, or None if
             inplace=True.
    """
    logger = get_logger(__name__)
    logger.info(
        f"Deduplicating DataFrame with subset={subset}, keep={keep}, "
        f"inplace={inplace}, ignore_index={ignore_index}"
    )

    df_pandas = df.pandas_api()
    result = df_pandas.drop_duplicates(
        subset=subset, keep=keep, inplace=inplace, ignore_index=ignore_index
    )

    if inplace:
        deduped_df = df_pandas.to_spark()
        logger.info(
            f"Deduplication complete (inplace). Number of rows after "
            f"deduplication: {deduped_df.count()}"
        )
        return None
    else:
        deduped_df = result.to_spark()
        logger.info(
            f"Deduplication complete. Number of rows after "
            f"deduplication: {deduped_df.count()}"
        )
        return deduped_df
