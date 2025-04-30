import pytest
import requests

@pytest.fixture(scope='function')
def provide_posts_data():
    user_id = 3
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    posts = response.json()
    valid_posts = [post for post in posts if post['userId'] == user_id and len(post['body']) > 0]

    if valid_posts:
        print(f"\n Found {len(valid_posts)} valid posts for userId={user_id}")
    else:
        print(f"\n No valid posts found for userId={user_id}!")

    return valid_posts

def test_user_with_posts(provide_posts_data):
    assert len(provide_posts_data) == 10, f"Expected 10 posts, but got {len(provide_posts_data)}"
    for post in provide_posts_data:
        assert post['userId'] == 3, f"Found post with unexpected userId: {post['userId']}"
    print("All posts belong to userId=3 and the total count is correct (10).")



def test_data_is_presented_between_staging_raw(list_gcs_blobs, list_aws_blobs):
    print(f"\n  GCP Blob count: {len(list_gcs_blobs)}")
    print(f"  AWS S3 Object count: {len(list_aws_blobs)}")

    assert len(list_gcs_blobs) > 0, "No data found in GCP bucket for the given date!"
    assert len(list_aws_blobs) > 0, "No data found in AWS S3 bucket for the given date!"

    print("Both GCP and AWS buckets contain data for the specified date.")




