from dagster import MultiPartitionsDefinition, Output, StaticPartitionsDefinition, asset

animals = (
    ["tiger", "camel", "zebra", "eagle"]
    + ["panda", "koala", "shark", "rhino"]
    + ["otter", "hippo", "sloth", "lemur"]
    + ["bunny", "sheep", "goose", "ferret"]
)
verbs = ["hugs", "tickles", "cuddles", "wiggles"] + ["juggles", "giggles", "befriends", "massages"]
objects = (
    ["donut", "pillow", "cookie", "bubble"]
    + ["karate", "banana", "marshm", "sprout"]
    + ["socks", "toycar", "cupcake", "flower"]
    + ["tomato", "feather", "guitar", "panda"]
)

alpha_items = [f"{a}_{b}_{c}" for a in animals for b in verbs for c in objects]
assert len(alpha_items) == 2048
alpha_partitions = StaticPartitionsDefinition(alpha_items)

adjectives = ["happy", "silly", "fuzzy", "goofy"]
nouns = ["panda", "otter", "robot", "koala"]
beta_items = [f"{adj}_{noun}" for adj in adjectives for noun in nouns]
assert len(beta_items) == 16
beta_partitions = StaticPartitionsDefinition(beta_items)

alpha_beta_partitions = MultiPartitionsDefinition(
    {"alpha": alpha_partitions, "beta": beta_partitions}
)

COUNT_OF_ALPHA = 10
COUNT_OF_BETA = 30
COUNT_OF_ZETA = 20


@asset(partitions_def=alpha_partitions)
def alpha_1(context) -> Output[int]:
    """An asset partitioned by alpha_partitions."""
    partition_key = context.partition_key
    context.log.info(f"Running alpha_1 for partition: {partition_key}")
    value = sum([ord(c) for c in partition_key]) % 256
    return Output(value, metadata={"value": value})


def make_alpha_asset(i: int):
    @asset(partitions_def=alpha_partitions, name=f"alpha_2_{i:02d}")
    def _alpha(context, alpha_1: int):
        partition_key = context.partition_key
        context.log.info(
            f"Running alpha_2_{i:02d} for partition: {partition_key} with input {alpha_1}"
        )
        value = alpha_1 * 2 * i
        return Output(value, metadata={"value": value})

    _alpha.__name__ = f"alpha_2_{i:02d}"
    _alpha.__doc__ = f"Auto-generated alpha_2_{i:02d} asset."
    return _alpha


for i in range(COUNT_OF_ALPHA):
    globals()[f"alpha_2_{i:02d}"] = make_alpha_asset(i)


@asset(partitions_def=beta_partitions)
def beta_1(context) -> Output[int]:
    """An asset partitioned by beta_partitions."""
    partition_key = context.partition_key
    context.log.info(f"Running beta_1 for partition: {partition_key}")
    value = sum([ord(c) for c in partition_key]) % 256
    return Output(value, metadata={"value": value})


def make_beta_asset(i: int):
    @asset(partitions_def=beta_partitions, name=f"beta_2_{i:02d}")
    def _beta(context, beta_1: int):
        partition_key = context.partition_key
        context.log.info(
            f"Running beta_2_{i:02d} for partition: {partition_key} with input {beta_1}"
        )
        value = beta_1 * 3 * i
        return Output(value, metadata={"value": value})

    _beta.__name__ = f"beta_2_{i:02d}"
    _beta.__doc__ = f"Auto-generated beta_2_{i:02d} asset."
    return _beta


for i in range(COUNT_OF_BETA):
    globals()[f"beta_2_{i:02d}"] = make_beta_asset(i)


@asset()
def zeta_1() -> Output[int]:
    """An asset with no partitions."""
    value = 10
    return Output(value, metadata={"value": value})


def make_zeta_asset(i: int):
    @asset(name=f"zeta_2_{i:02d}")
    def _zeta(zeta_1: int):
        value = zeta_1 + 5 * i
        return Output(value, metadata={"value": value})

    _zeta.__name__ = f"zeta_2_{i:02d}"
    _zeta.__doc__ = f"Auto-generated zeta_2_{i:02d} asset."
    return _zeta


for i in range(COUNT_OF_ZETA):
    globals()[f"zeta_2_{i:02d}"] = make_zeta_asset(i)


from dagster import AssetIn, MultiPartitionsDefinition, Output, asset

# Collect dynamic input mapping for all 60 assets
sigma_inputs = (
    {f"alpha_2_{i:02d}": AssetIn(key=f"alpha_2_{i:02d}") for i in range(COUNT_OF_ALPHA)}
    | {f"beta_2_{i:02d}": AssetIn(key=f"beta_2_{i:02d}") for i in range(COUNT_OF_BETA)}
    | {f"zeta_2_{i:02d}": AssetIn(key=f"zeta_2_{i:02d}") for i in range(COUNT_OF_ZETA)}
)


@asset(
    partitions_def=alpha_beta_partitions,
    ins=sigma_inputs,
)
def sigma(context, **kwargs) -> Output[float]:
    # kwargs contains { "alpha_2_00": int, "beta_2_00": int, ..., "zeta_2_19": int }

    partition = context.partition_key.keys_by_dimension
    alpha_key = partition.get("alpha")
    beta_key = partition.get("beta")

    # Logging details
    context.log.info(
        f"Running sigma for alpha={alpha_key}, beta={beta_key} with {len(kwargs)} inputs"
    )

    # Aggregate result (simple example)
    avg_value = sum(kwargs.values()) / len(kwargs)

    return Output(avg_value, metadata={"value": avg_value})
