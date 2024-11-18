from atproto import Client, client_utils
import os

def main():
    client = Client()
    profile = client.login(os.getenv('BSKY_USERNAME'), os.getenv('BSKY_PASSWORD'))
    print('Welcome,', profile.display_name)

    text = client_utils.TextBuilder().text('Hello World from ').link('Python SDK', 'https://atproto.blue')
    post = client.send_post(text)
    client.like(post.uri, post.cid)


if __name__ == '__main__':
    main()

