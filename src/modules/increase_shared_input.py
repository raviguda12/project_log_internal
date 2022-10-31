import pandas as pd
import math
import random
import env
from helpers.s3_helper import S3Session
import os


def get_data_df(input_file_path):
    """
    The get_data_df function reads in a file and returns a dataframe with the first 300 rows.
    The function also adds two new columns to the dataframe, one for each of the two new features:
        1) The number of words in an essay (num_words)
        2) The number of sentences in an essay (num_sentences).


    :param input_file_path: Specify the path of the input file
    :return: A dataframe with the first 300 rows of the input file
    """
    df = pd.read_csv(input_file_path, sep=' ', header=None)
    df[8] = df[8].apply(lambda x: add_str(x))
    df[10] = df[10].apply(lambda x: add_str(x))
    return df.iloc[:300]


def add_str(val):
    """
    The add_str function adds a string to the end of a value.

    :param val: Pass a value that will be added to the string
    :return: A string with a space in front of the parameter
    """
    return r" {}".format(val)


def read_file_from_s3(session, bucket_name, download_path, s3_file_path, run_once):
    """
    The read_file_from_s3 function downloads a file from an S3 bucket and saves it to the local machine.

    :param session: Create a connection to the aws service
    :param bucket_name: Specify the name of the bucket that contains the file
    :param download_path: Specify the path where the file will be downloaded
    :param s3_file_path: Specify the file path of the file to be downloaded
    :param run_once: Determine whether the function should download all files from s3 or stop after downloading one file
    :return: A file from s3
    """
    s3_resource = session.resource('s3')
    s3_client = session.client('s3')
    bucket = s3_resource.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=s3_file_path))
    for obj in objs:
        _ = True if os.path.isdir(download_path) else os.mkdir(download_path)
        s3_client.download_file(bucket_name, obj.key, r"{}/{}".format(download_path, obj.key.split(r"/")[-1]))
        if run_once:
            break


def get_required_var_for_iterations(data_df_row_size, rows_to_be_populated):
    """
    The get_required_var_for_iterations function takes in the size of the dataframe and
    the number of rows to be populated into the dataframe. It returns a tuple containing:
        1) The number of times that we need to iterate over our dataframe (loop_iterations_to_df).
        2) A list containing all rows that are divisible by 10, which will be used as indices for populating our df.
           This is done because it's easier to populate a df with values from 0-9 rather than 1-10, since there are no values between 9 and 10.
           We avoid index

    :param data_df_row_size: Calculate the number of rows to be populated in each iteration
    :param rows_to_be_populated: Determine how many rows to be populated in the dataframe
    :return: A tuple of the following:
    """
    df_row_size = data_df_row_size
    loop_iterations_to_df = math.ceil(rows_to_be_populated / df_row_size)
    rows_to_pick = list(filter(lambda val: val % 10 == 0, [*range(0, df_row_size)]))[1:]
    rows_to_avoid = [0] + list(filter(lambda val: val % 10 != 0, [*range(0, df_row_size)]))
    return loop_iterations_to_df, rows_to_pick, rows_to_avoid


def increase_ip(ip_val):
    """
    The increase_ip function takes an IP address and increments the last octet by 1.
    For example, if the input is 192.168.0.2, then it will return 192.168.0.3.

    :param ip_val: Store the ip address that is passed to the function
    :return: The incremented ip address
    """
    ip_len = ip_val.split(".")
    return ".".join([str(int(val) + 1) if (index == len(ip_len) - 1) else val for index, val in enumerate(ip_len)])


def clean_request(request_val):
    """
    The clean_request function takes a request value and replaces it with a random choice from the method variations list.
    The method variations list is created by randomly selecting one of the three GET, POST, or HEAD strings.

    :param request_val: Pass the request value to the function
    :return: A string with a random http method and the same url
    """
    remove_char_list = ['%', '-', '|', ","]
    method_variations_list = ["GET", "POST", "HEAD"]
    splitted_request_val = request_val.split(" ")
    splitted_request_val[0] = random.choice(method_variations_list)
    splitted_request_val = " ".join(splitted_request_val)
    return splitted_request_val


def clean_referer(referer_val):
    """
    The clean_referer function takes a string and removes any of the following characters:
        %, -, |, ,


    :param referer_val: Remove the unwanted characters from the referer_val parameter
    :return: The referer_val with the following characters removed:
    """
    remove_char_list = ['%', '-', '|', ","]
    return "".join(["" if request_char in remove_char_list else request_char for request_char in referer_val])


def replace_rows(df):
    """
    The replace_rows function takes a dataframe as an argument and returns the same dataframe with
    the following changes:
        1. The IP address is converted to an integer by using the ip2int function, which converts each octet into
        its corresponding integer value. This allows for easier sorting of IP addresses in later steps.

    :param df: Pass the dataframe to be manipulated
    :return: A new dataframe with the following transformations:
    """
    df[0] = df[0].apply(lambda x: increase_ip(x))
    df[3] = pd.to_datetime(df[3], format="[%d/%b/%Y:%H:%M:%S") + pd.Timedelta(hours=1)
    df[3] = df[3].dt.strftime('[%d/%b/%Y:%H:%M:%S')
    df[5] = df[5].apply(lambda x: clean_request(x))
    return df


def remove_space(save_to_dummy_path, save_to_path):
    """
    The remove_space function removes the space between the first and second line of each
    paragraph. This is done to make it easier for me to read through my generated ReST files.

    :param save_to_dummy_path: Save the original file to a dummy path
    :param save_to_path: Save the file to a specific path
    :return: The file with spaces removed
    """
    all_lines = []
    dummy_file_lines = open(save_to_dummy_path, 'r').readlines()
    for line in dummy_file_lines:
        temp_line = line.split('"')
        temp_line[3] = temp_line[3].strip()
        temp_line[-2] = temp_line[-2].strip()
        changed_line = '"'.join(temp_line)
        all_lines.append(changed_line)
    save_original_file = open(save_to_path, 'w')
    save_original_file.writelines(all_lines)


def populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path, save_to_dummy_path):
    """
    The populate_file function takes in the following parameters:
        1. loop_iterations_to_df - The number of times to run through the dataframe and replace rows with random values
        2. rows_to_pick - The number of rows to replace with random values per iteration
        3. rows_to_avoid - The number of original, unaltered, non-randomized data points that should be avoided per iteration
            (this is done by randomly selecting a subset from the original dataset)
            This parameter is used so that we don't end up replacing all our randomized data points with new randomized

    :param loop_iterations_to_df: Specify the number of iterations to run the populate_file function
    :param rows_to_pick: Specify the number of rows to be replaced in each iteration
    :param rows_to_avoid: Avoid picking the same rows from the original dataframe
    :param data_df: Pass the dataframe that is to be populated
    :param save_to_path: Save the file to a specific path
    :param save_to_dummy_path: Save the modified dataframe to a dummy file
    :return: The number of rows populated in the csv file
    """
    modified_df = data_df
    df_row_count = 0
    _ = os.remove(save_to_path) if os.path.exists(save_to_path) else True
    _ = os.remove(save_to_dummy_path) if os.path.exists(save_to_dummy_path) else True
    _ = True if os.path.isdir(r"/".join(save_to_path.split(r"/")[:-1])) else os.mkdir(
        r"/".join(save_to_path.split(r"/")[:-1]))
    _ = True if os.path.isdir(r"/".join(save_to_dummy_path.split(r"/")[:-1])) else os.mkdir(
        r"/".join(save_to_dummy_path.split(r"/")[:-1]))
    for file_index in range(0, loop_iterations_to_df):
        if file_index > 0:
            shuffled_df = modified_df.sample(frac=1)
            replaced_df = replace_rows(shuffled_df.iloc[rows_to_pick])
            unchanged_shuffled_df = shuffled_df.iloc[rows_to_avoid]
            modified_df = pd.concat([unchanged_shuffled_df, replaced_df], axis=0).sample(frac=1)
            modified_df.to_csv(save_to_dummy_path, header=None, index=None, sep=' ', mode="a")
            remove_space(save_to_dummy_path, save_to_path)
            df_row_count += modified_df.shape[0]
            print("{} iterations completed and {} rows populated".format(file_index, df_row_count))
        else:
            modified_df.to_csv(save_to_dummy_path, header=None, index=None, sep=' ', mode="a")
            remove_space(save_to_dummy_path, save_to_path)
            df_row_count += modified_df.shape[0]
            print("{} iterations completed and {} rows populated".format(file_index, df_row_count))


if __name__ == "__main__":
    read_file_from_s3(S3Session.get_boto3_session(),
                      env.s3_bucket,
                      "{}/{}".format(os.getcwd(), env.shared_input_path),
                      env.s3_shared_input_path,
                      True)
    input_file_path = r"{}/{}/{}".format(os.getcwd(), env.shared_input_path, env.shared_input_file)
    rows_to_be_populated = env.processed_input_count
    save_to_path = r"{}/{}/{}".format(os.getcwd(), env.processed_input_path, env.processed_input_file)
    save_to_dummy_path = r"{}/{}/{}".format(os.getcwd(), env.processed_input_path, env.dummy_processed_input_file)
    data_df = get_data_df(input_file_path)
    loop_iterations_to_df, rows_to_pick, rows_to_avoid = get_required_var_for_iterations(data_df.shape[0],
                                                                                         rows_to_be_populated)
    populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path, save_to_dummy_path)
