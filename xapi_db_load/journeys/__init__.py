"""
Learner-journey data generation.

Unlike the random event mode, which picks every event independently, this mode simulates each
enrolled learner moving through a properly nested course: navigating units in order, attempting
the problems and watching the videos in them, in time-ordered sessions. Because the generator knows
exactly what each learner did, it also writes the expected engagement results, so reports can be
checked against a known answer.
"""
