"""Operations that keep a dataset's type, generic over the traits they need.

Each function takes a dataset bounded by traits, such as ``D`` bound to
``Dataset[Association]``, and returns the same dataset type, so it works on every
dataset with those traits. Call it directly or chain it with ``Dataset.pipe``.
Operations that turn one dataset into another, such as clumping, are methods in
``gentropy.method`` instead.
"""
