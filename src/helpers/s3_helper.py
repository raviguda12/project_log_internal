import boto3
import env


class S3Helper:
    def get_boto3_session():
        """
        The get_boto3_session function creates a boto3 session object using the AWS credentials
        stored in the .env file. The function returns this session object.

        :return: A boto3 session object
        """
        # Creating Session With Boto3.
        return boto3.Session(
            aws_access_key_id=env.aws_access_key_id.lstrip("**"),
            aws_secret_access_key=env.aws_secret_access_key.lstrip("**")
        )
