view: gm_variance {
  derived_table: {
    sql: WITH
            base_data AS (
              SELECT
                ii.product_id,
                p.category AS product_category,
                oi.sale_price,
                ii.cost,
                oi.returned_at,
                ({% condition dates_period_1 %} oi.created_at {% endcondition %}) AS is_period_1,
                ({% condition dates_period_2 %} oi.created_at {% endcondition %}) AS is_period_2
              FROM `looker-private-demo.ecomm.order_items` AS oi
              LEFT JOIN `looker-private-demo.ecomm.inventory_items` AS ii
                ON oi.inventory_item_id = ii.id
              LEFT JOIN `looker-private-demo.ecomm.products` AS p
                ON ii.product_id = p.id
              WHERE
                ({% condition dates_period_1 %}
                  oi.created_at  {% endcondition %}
                  OR {% condition dates_period_2 %} oi.created_at  {% endcondition %}
                )
            ),
            aggregated_data AS (
              SELECT
                product_id,
                product_category,
                SUM(CASE WHEN is_period_1 THEN CASE WHEN returned_at IS NULL THEN 1 ELSE -1 END ELSE 0 END)
                  AS quantity_c1,
                SUM(CASE WHEN is_period_2 THEN CASE WHEN returned_at IS NULL THEN 1 ELSE -1 END ELSE 0 END)
                  AS quantity_c2,
                SUM(CASE WHEN is_period_1 THEN sale_price ELSE 0 END) AS revenue_c1,
                SUM(CASE WHEN is_period_2 THEN sale_price ELSE 0 END) AS revenue_c2,
                SUM(CASE WHEN is_period_1 THEN cost ELSE 0 END) AS cost_c1,
                SUM(CASE WHEN is_period_2 THEN cost ELSE 0 END) AS cost_c2
              FROM base_data
              GROUP BY 1, 2
            ),
            matching_product AS (
              SELECT
                product_id,
                (CASE WHEN SUM(quantity_c1) > 0 AND SUM(quantity_c2) > 0 THEN 1 ELSE 0 END)
                  AS matching_product
              FROM aggregated_data
              GROUP BY 1
            ),
            data_with_matching_product AS (
              SELECT
                agg.*,
                COALESCE(mp.matching_product, 0) AS matching_product
              FROM aggregated_data AS agg
              LEFT JOIN matching_product AS mp
                ON agg.product_id = mp.product_id
            ),
            final_data AS (
              SELECT
                product_id,
                product_category,
                matching_product,
                quantity_c1,
                quantity_c2,
                revenue_c1,
                revenue_c2,
                cost_c1,
                cost_c2,
                revenue_c1 - cost_c1 AS gross_margin_c1,
                revenue_c2 - cost_c2 AS gross_margin_c2,  -- Per-unit metrics
                CASE WHEN quantity_c1 = 0 THEN 0 ELSE revenue_c1 / quantity_c1 END AS price_per_unit_c1,
                CASE WHEN quantity_c2 = 0 THEN 0 ELSE revenue_c2 / quantity_c2 END AS price_per_unit_c2,
                CASE
                  WHEN quantity_c1 = 0 THEN 0
                  ELSE (revenue_c1 - cost_c1) / quantity_c1
                  END AS margin_per_unit_c1,
                CASE
                  WHEN quantity_c2 = 0 THEN 0
                  ELSE (revenue_c2 - cost_c2) / quantity_c2
                  END AS margin_per_unit_c2
              FROM data_with_matching_product
            )
          SELECT
            *,
            DATE_DIFF(DATE({% date_end dates_period_1 %}), DATE({% date_start dates_period_1 %}), MONTH)
              AS months_c1,
            DATE_DIFF(DATE({% date_end dates_period_2 %}), DATE({% date_start dates_period_2 %}), MONTH)
              AS months_c2
          FROM final_data
        ;;
  }

# 1. Define the Filters used in the {% condition %} tags
  filter: dates_period_1 {
    type: date
    label: "Date Range Period 1"
    description: "Select the first date range for comparison."
  }

  filter: dates_period_2 {
    type: date
    label: "Date Range Period 2"
    description: "Select the second date range for comparison."
  }

  # 2. Define Dimensions for each column in the derived table's SELECT statement

  dimension: quantity_c1 {
    hidden: yes
    type: number
    sql: ${TABLE}.quantity_c1 ;;
  }

  dimension: quantity_c2 {
    hidden: yes
    type: number
    sql: ${TABLE}.quantity_c2 ;;
  }

  dimension: months_c1 {
    hidden: yes
    type: number
    sql: ${TABLE}.months_c1 ;;
  }

  dimension: months_c2 {
    hidden: yes
    type: number
    sql: ${TABLE}.months_c2 ;;
  }

  dimension: margin_per_unit_c1 {
    hidden: yes
    type: number
    sql: ${TABLE}.margin_per_unit_c1 ;;
  }

  dimension: margin_per_unit_c2 {
    hidden: yes
    type: number
    sql: ${TABLE}.margin_per_unit_c2 ;;
  }

  dimension: matching_product {
    hidden: yes
    type: number
    sql: ${TABLE}.matching_product ;;
  }

  dimension: price_per_unit_c1 {
    hidden: yes
    type: number
    sql: ${TABLE}.price_per_unit_c1 ;;
  }

  dimension: price_per_unit_c2 {
    hidden: yes
    type: number
    sql: ${TABLE}.price_per_unit_c2 ;;
  }

  dimension: gross_margin_c1 {
    hidden: yes
    type: number
    sql: ${TABLE}.gross_margin_c1 ;;
  }

  dimension: gross_margin_c2 {
    hidden: yes
    type: number
    sql: ${TABLE}.gross_margin_c2 ;;
  }

  dimension: revenue_c1 {
    type: number
    sql: ${TABLE}.revenue_c1 ;;
  }

  dimension: revenue_c2 {
    type: number
    sql: ${TABLE}.revenue_c2 ;;
  }


  dimension: product_id {
    label: "Product ID"
    type: number
    sql: ${TABLE}.product_id ;;
    description: "ID of the product."
    value_format_name: id
  }

  dimension: product_category {
    type: string
    sql: ${TABLE}.product_category ;;
    description: "Category of the product."
    drill_fields: [product_details*]
  }

  dimension: sale_price {
    hidden: yes
    type: number
    value_format_name: usd # Example format
    sql: ${TABLE}.sale_price ;;
    description: "The sale price of the item."
  }

  dimension: cost {
    hidden: yes
    type: number
    value_format_name: usd # Example format
    sql: ${TABLE}.cost ;;
    description: "The cost of the item."
  }

  dimension: returned_at {
    hidden: yes
    type: date_time
    sql: ${TABLE}.returned_at ;;
    description: "Timestamp when the item was returned."
  }

  dimension: is_period_1 {
    hidden: yes
    type: yesno
    sql: ${TABLE}.is_period_1 ;;
    description: "Is the order creation date within Period 1?"
  }

  dimension: is_period_2 {
    hidden: yes
    type: yesno
    sql: ${TABLE}.is_period_2 ;;
    description: "Is the order creation date within Period 2?"
  }

  # Example Measures
  measure: total_sales {
    type: sum
    sql: ${revenue_c1} + ${revenue_c2} ;;
    value_format_name: usd
  }

  measure: total_sales_period_1 {
    type: sum
    sql: ${revenue_c1} ;;
    value_format_name: usd
    label: "Total Sales (Period 1)"
  }

  measure: total_sales_period_2 {
    type: sum
    sql: ${revenue_c2} ;;
    value_format_name: usd
    label: "Total Sales (Period 2)"
  }

  measure: total_gross_margin_c1 {
    type: sum
    sql: ${gross_margin_c1} ;;
    value_format_name: usd
    label: "Total Gross Margin (Period 1)"
  }

  measure: total_gross_margin_c2 {
    type: sum
    sql: ${gross_margin_c2} ;;
    value_format_name: usd
    label: "Total Gross Margin (Period 2)"
  }

  measure: volume_effect {
    description: "How much more/less profit did we make just from selling more/fewer units, holding everything else constant?"
    type: sum
    sql: ((${quantity_c1} / NULLIF(${months_c1},0)) - (${quantity_c2} / NULLIF(${months_c2},0))) * ${margin_per_unit_c2} * ${matching_product} ;;
    value_format_name: usd
    drill_fields: [product_details*]
  }

  measure: price_effect {
    description: "How much more/less profit did we make from price changes, using Period 1 volume as the base?"
    type: sum
    sql: (${price_per_unit_c1} - ${price_per_unit_c2}) * (${quantity_c1} / NULLIF(${months_c1},0)) * ${matching_product} ;;
    value_format_name: usd
    drill_fields: [product_details*]
  }

  measure: product_mix_effect {
    description: "How much more/less profit did we make from changes in product profitability (cost changes, promotional activity, etc.)?"
    type: sum
    sql: (${margin_per_unit_c1} - ${margin_per_unit_c2}) * (${quantity_c1} / NULLIF(${months_c1},0)) * ${matching_product} ;;
    value_format_name: usd
    drill_fields: [product_details*]
  }

  measure: non_matching_products {
    description: "What’s the profit impact from products that only existed in one period (new products or discontinued products)?"
    type: sum
    sql: ((${gross_margin_c1} / NULLIF(${months_c1},0)) - (${gross_margin_c2} / NULLIF(${months_c2},0))) * (1 - ${matching_product}) ;;
    value_format_name: usd
    drill_fields: [product_details*]
  }

  measure: count {
    type: count
    drill_fields: [product_id, product_category]
  }

  set: product_details {
    fields: [product_category, product_id, products.item_name, total_sales_period_1, total_sales_period_2, total_gross_margin_c1, total_gross_margin_c2, volume_effect, price_effect, product_mix_effect, non_matching_products]
  }
}
