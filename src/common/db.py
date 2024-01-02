

def read_from_db(connection_type, connection_options):

    data_df = glue_context.create_dynamic_frame.from_options(
            connection_type=connection_type,
            connection_options=connection_options
        )
    
    return data_df