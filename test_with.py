class TestWith:
    def __init__(self, number: int):
        self.number = number

    def __enter__(self):
        if self.number == 10:
            raise Exception(self.number)
        return self.number

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(self.number)


if __name__ == '__main__':
    with TestWith(10) as (result, err):
        if err:
            print("Have error")
        else:
            print(result)
    exit(1)
