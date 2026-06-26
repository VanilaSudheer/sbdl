# lib/Transformations.py

from pyspark.sql.functions import (
    col,
    lit,
    struct,
    array,
    when,
    isnull,
    filter,
    collect_list,
    expr,
    current_timestamp
)


def get_insert_operation(column, alias):
    return struct(
        lit("INSERT").alias("operation"),
        column.alias("newValue"),
        lit(None).alias("oldValue")
    ).alias(alias)


def get_contract(df):
    contract_title = array(
        when(
            ~isnull("legal_title_1"),
            struct(
                lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                col("legal_title_1").alias("contractTitleLine")
            )
        ),
        when(
            ~isnull("legal_title_2"),
            struct(
                lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                col("legal_title_2").alias("contractTitleLine")
            )
        )
    )

    contract_title_nl = filter(contract_title, lambda x: ~isnull(x))

    tax_identifier = struct(
        col("tax_id_type").alias("taxIdType"),
        col("tax_id").alias("taxId")
    ).alias("taxIdentifier")

    return df.select(
        "account_id",
        get_insert_operation(col("account_id"), "contractIdentifier"),
        get_insert_operation(col("source_sys"), "sourceSystemIdentifier"),
        get_insert_operation(col("account_start_date"), "contactStartDateTime"),
        get_insert_operation(contract_title_nl, "contractTitle"),
        get_insert_operation(tax_identifier, "taxIdentifier"),
        get_insert_operation(col("branch_code"), "contractBranchCode"),
        get_insert_operation(col("country"), "contractCountry")
    )


def get_relations(df):
    return df.select(
        "account_id",
        "party_id",
        get_insert_operation(col("party_id"), "partyIdentifier"),
        get_insert_operation(col("relation_type"), "partyRelationshipType"),
        get_insert_operation(col("relation_start_date"), "partyRelationStartDateTime")
    )


def get_address(df):
    address = struct(
        col("address_line_1").alias("addressLine1"),
        col("address_line_2").alias("addressLine2"),
        col("city").alias("addressCity"),
        col("postal_code").alias("addressPostalCode"),
        col("country_of_address").alias("addressCountry"),
        col("address_start_date").alias("addressStartDate")
    )

    return df.select(
        "party_id",
        get_insert_operation(address, "partyAddress")
    )


def join_party_address(p_df, a_df):
    return p_df.join(a_df, "party_id", "left_outer") \
        .groupBy("account_id") \
        .agg(
            collect_list(
                struct(
                    "partyIdentifier",
                    "partyRelationshipType",
                    "partyRelationStartDateTime",
                    "partyAddress"
                )
            ).alias("partyRelations")
        )


def join_contract_party(c_df, p_df):
    return c_df.join(p_df, "account_id", "left_outer")


def apply_header(spark, df):
    event_header = struct(
        expr("uuid()").alias("eventIdentifier"),
        lit("SBDL-Contract").alias("eventType"),
        lit(1).alias("majorSchemaVersion"),
        lit(0).alias("minorSchemaVersion"),
        current_timestamp().alias("eventDateTime")
    ).alias("eventHeader")

    keys = array(
        struct(
            lit("contractIdentifier").alias("keyField"),
            col("contractIdentifier.newValue").alias("keyValue")
        )
    ).alias("keys")

    payload = struct(
        "contractIdentifier",
        "sourceSystemIdentifier",
        "contactStartDateTime",
        "contractTitle",
        "taxIdentifier",
        "contractBranchCode",
        "contractCountry",
        "partyRelations"
    ).alias("payload")

    return df.select(
        event_header,
        keys,
        payload
    )