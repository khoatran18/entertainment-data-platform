
def get_valid_invalid_path(
        parent_path,
        data_type
):
    valid_path = f"{parent_path.valid_base_path}/{data_type}"
    invalid_path = f"{parent_path.invalid_base_path}/{data_type}"
    return valid_path, invalid_path
