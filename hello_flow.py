# hello_flow.py
from prefect import flow


@flow
def hello():
    print("Hello, Prefect!")


if __name__ == "__main__":
    hello()
