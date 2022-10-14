# see the .dbtignore file in the root of the repo -- this .py file is ignored by dbt parsing!
# use this is you have `.py` files in models/ that aren't models, for some reason


def main():
    print("I'm ignored by dbt!")


if __name__ == "__main__":
    # call the main function
    main()
