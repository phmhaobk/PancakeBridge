from mapping import *
from datetime import date

def execute_pipeline(engine):
    steps = [
        ("execute_insert_hashtag_mapping", execute_insert_hashtag_mapping, []),
        ("insert_hoithoai_mapping", insert_hoithoai_mapping, []),
        ("update_loai_khach_hang", update_loai_khach_hang, []),
        ("update_level_khach_hang", update_level_khach_hang, [engine]),
        ("update_level_khach_hang_K0", update_level_khach_hang_K0, [engine]),
        ("def_updated_C3_KN_1", def_updated_C3_KN_1, [engine]),
        ("def_updated_C3_KN_2", def_updated_C3_KN_2, [engine]),
        ("def_updated_C3_KN_3", def_updated_C3_KN_3, [engine]),
        ("def_updated_C2", def_updated_C2, [engine]),
        ("process_conversations_case_6", process_conversations_case_6, [engine]),
        ("process_conversations_case_5", process_conversations_case_5, [engine]),
        ("def_updated_C2_v2", def_updated_C2_v2, [engine])
    ]

    try:
        with engine.connect() as conn:
            with conn.begin():  # Begin transaction
                for step_name, step_func, args in steps:
                    print(f"Executing step: {step_name}")
                    step_func(*args)  # Execute the function with arguments
                    print(f"Step completed: {step_name}")
                    
    except Exception as e:
        print(f"Pipeline failed at step {step_name}: {e}")
        raise  # Re-raise the exception to handle it externally
execute_pipeline(engine)