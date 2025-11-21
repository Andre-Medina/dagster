from dagster import MultiPartitionsDefinition, Output, StaticPartitionsDefinition, asset

alpha_partitions = StaticPartitionsDefinition(["A", "B", "C"])
beta_partitions = StaticPartitionsDefinition(["a", "b", "c"])


@asset(partitions_def=alpha_partitions)
def alpha_1(context) -> Output[int]:
    """An asset partitioned by alpha_partitions."""
    partition_key = context.partition_key
    context.log.info(f"Running alpha_1 for partition: {partition_key}")
    value = ord(partition_key)
    return Output(value, metadata={"value": value})


@asset(partitions_def=alpha_partitions)
def alpha_2(context, alpha_1: int) -> Output[int]:
    """An asset partitioned by alpha_partitions, depends on alpha_1."""
    partition_key = context.partition_key
    context.log.info(f"Running alpha_2 for partition: {partition_key} with input {alpha_1}")
    value = alpha_1 * 2
    return Output(value, metadata={"value": value})


@asset(partitions_def=beta_partitions)
def beta_1(context) -> Output[int]:
    """An asset partitioned by beta_partitions."""
    partition_key = context.partition_key
    context.log.info(f"Running beta_1 for partition: {partition_key}")
    value = ord(partition_key)
    return Output(value, metadata={"value": value})


@asset(partitions_def=beta_partitions)
def beta_2(context, beta_1: int) -> Output[int]:
    """An asset partitioned by beta_partitions, depends on beta_1."""
    partition_key = context.partition_key
    context.log.info(f"Running beta_2 for partition: {partition_key} with input {beta_1}")
    value = beta_1 * 3
    return Output(value, metadata={"value": value})


@asset()
def zeta_1() -> Output[int]:
    """An asset with no partitions."""
    value = 10
    return Output(value, metadata={"value": value})


@asset()
def zeta_2(zeta_1: int) -> Output[int]:
    """An asset with no partitions, depends on zeta_1."""
    value = zeta_1 + 5
    return Output(value, metadata={"value": value})


# Stage 3 Aggregation Asset
@asset(
    partitions_def=MultiPartitionsDefinition({"alpha": alpha_partitions, "beta": beta_partitions}),
)
def sigma(context, alpha_2: int, beta_2: int, zeta_2: int) -> Output[float]:
    """An aggregation asset with multi-partitions, depends on alpha_2, beta_2, and zeta_2."""
    alpha_key = context.partition_key.keys_by_dimension["alpha"]
    beta_key = context.partition_key.keys_by_dimension["beta"]
    context.log.info(
        f"Running sigma for alpha: {alpha_key}, beta: {beta_key} "
        f"with inputs alpha_2={alpha_2}, beta_2={beta_2}, zeta_2={zeta_2}"
    )
    value = (alpha_2 + beta_2 + zeta_2) / 3.0
    return Output(value, metadata={"value": value})
