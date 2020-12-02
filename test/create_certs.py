import zmq.auth
import argparse
import os

keys_dir = ".curve"

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--force", action='store_true',
                        help="Force overwrite if keys already exist")
    parser.add_argument("-d", "--dir", default='.curve',
                        help="Directory to which keys should be written.")
    args = parser.parse_args()

    os.makedirs(args.dir, exist_ok=True)

    target = os.path.join(args.dir, 'server.key')
    if os.path.exists(target):
        if args.force:
            print(f"Server keys exist at: {target}*, overwriting")
            server_public_file, server_secret_file = zmq.auth.create_certificates(args.dir, "server")
            print(f"Wrote key to {server_secret_file}")
        else:
            print(f"Server keys exist at: {target}*, aborting")
