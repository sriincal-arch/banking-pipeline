{% macro audit_columns() %}
    current_timestamp as created_date,
    current_timestamp as modified_date,
    json_object(
        'source_file', _source_file,
        'row_number', _row_number,
        'file_hash', _file_hash,
        'ingested_at', _ingested_at
    ) as metadata_json
{% endmacro %}


{% macro audit_columns_incremental(unique_key) %}
    {% if is_incremental() %}
        coalesce(existing.created_date, current_timestamp) as created_date,
        current_timestamp as modified_date,
        case 
            when existing.metadata_json is not null 
            then json_patch(
                existing.metadata_json,
                json_object(
                    'history', json_array_append(
                        coalesce(json_extract(existing.metadata_json, '$.history'), '[]'),
                        json_object(
                            'source_file', _source_file,
                            'row_number', _row_number,
                            'file_hash', _file_hash,
                            'ingested_at', _ingested_at,
                            'modified_at', current_timestamp
                        )
                    )
                )
            )
            else json_object(
                'source_file', _source_file,
                'row_number', _row_number,
                'file_hash', _file_hash,
                'ingested_at', _ingested_at,
                'history', '[]'
            )
        end as metadata_json
    {% else %}
        current_timestamp as created_date,
        current_timestamp as modified_date,
        json_object(
            'source_file', _source_file,
            'row_number', _row_number,
            'file_hash', _file_hash,
            'ingested_at', _ingested_at,
            'history', '[]'
        ) as metadata_json
    {% endif %}
{% endmacro %}


