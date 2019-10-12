from numpy import *
import scipy.stats
import math

w1_x1 = [-5.01, -5.43, 1.08, 0.86, -2.67, 4.94, -2.51, -2.25, 5.56, 1.03]
w2_x1 = [-0.91, 1.30, -7.75, -5.47, 6.14, 3.60, 5.37, 7.18, -7.39, -7.50]
w3_x1 = [5.35, 5.12, -1.34, 4.48, 7.11, 7.17, 5.75, 0.77, 0.90, 3.52]

u_w1_x1 = average(array(w1_x1))
u_w2_x1 = average(array(w2_x1))
u_w3_x1 = average(array(w3_x1))

sigma_square_w1_x1 = var(array(w1_x1))
sigma_square_w2_x1 = var(array(w2_x1))
sigma_square_w3_x1 = var(array(w3_x1))

print("u1: ", u_w1_x1, "sigma: ", sigma_square_w1_x1)
print("u2: ", u_w2_x1, "sigma: ", sigma_square_w2_x1)

norm1 = scipy.stats.norm(u_w1_x1, sigma_square_w1_x1)
norm2 = scipy.stats.norm(u_w2_x1, sigma_square_w2_x1)


# helper function for (a)
def dichotomizer(i, likelyhoodFunc1, likelyhoodFunc2):
    return math.log2(likelyhoodFunc1.cdf(i) / likelyhoodFunc2.cdf(i)) + math.log2(0.5 / 0.5)


# answer for (a)
answer1 = lambda i: dichotomizer(i, norm1, norm2)


def classify_sample(sample):
    res = []
    for each in sample:
        if answer1(each) > 0:
            res.append("w1")
        else:
            res.append("w2")
    return res


# classified result for each class
classified_w1_x1 = classify_sample(w1_x1)
classified_w2_x1 = classify_sample(w2_x1)
print("The classified x1 in class w1", classified_w1_x1, "\n", "The classified x1 in class w2", classified_w2_x1)


def error_rate(classified_data, category):
    count = 0
    for each in classified_data:
        if each != category:
            count += 1
    return float(count / len(classified_data))


# answer for (b)
error_rate_for_w1_x1 = error_rate(classified_w1_x1, "w1")
error_rate_for_w2_x1 = error_rate(classified_w2_x1, "w2")
print("experimental error rates for w1 and w2: ", error_rate_for_w1_x1, error_rate_for_w2_x1)

# answer for (c)
bhattacharyya = lambda u1, sigma1_square, u2, sigma2_square: 0.25 * math.log2(
    0.25 * (sigma1_square / sigma2_square + sigma2_square / sigma1_square + 2)) + 0.25 * (
                                                                     math.pow((u1 - u2), 2) / (
                                                                         sigma1_square + sigma2_square))

k = bhattacharyya(u_w1_x1, sigma_square_w1_x1, u_w2_x1, sigma_square_w2_x1)
print("k value: ", k)
p_error = math.sqrt(0.5 * 0.5) * math.pow(math.e, k * (-1))
print("Using Bhattacharyya, maximum error is ", p_error)
