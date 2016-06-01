d1 = read.table("sq_job_means_w1_t2_s2.0_sf2_af1.dat")
d2 = read.table("sq_job_means_w2_t4_s2.0_sf2_af1.dat")
d4 = read.table("sq_job_means_w4_t8_s2_sf2_af1.dat")
d8 = read.table("sq_job_means_w8_t16_s2_sf2_af1.dat")
d16 = read.table("sq_job_means_w16_t32_s2_sf2_af1.dat")
d32 = read.table("sq_job_means_w32_t64_s2_sf2_af1.dat")
d64 = read.table("sq_job_means_w64_t128_s2_sf2_af1.dat")

colors = rainbow(length(d1$V1))

u = d1$V5[1]
l = d1$V5[1]
for (i in 1:length(d1$V1)) {
    l = min(l, d1$V5[i], d2$V5[i], d4$V5[i], d8$V5[i], d16$V5[i], d32$V5[i], d64$V5[i])
    u = max(u, d1$V5[i], d2$V5[i], d4$V5[i], d8$V5[i], d16$V5[i], d32$V5[i], d64$V5[i])
}

plot(c(), c(), ylim=range(0,u), xlim=range(0,6))
title("log2(workers) vs sojourn time for 2x task:worker ratio")

fits = matrix(0,9,2)

for (i in 1:length(d1$V1)) {
    print(i)
    v = c(d1$V5[i], d2$V5[i], d4$V5[i], d8$V5[i], d16$V5[i], d32$V5[i], d64$V5[i])
    print(cor(c(0:(length(v)-1)), v))
    fit = lm(v ~ c(0:(length(v)-1)))
    print(fit$coefficients)
    fits[i,1] = fit$coefficients[1]
    fits[i,2] = fit$coefficients[2]
    points(c(0:(length(v)-1)), v)
    abline(fit, col=colors[i])
}

ll=d1$V2*d1$V3/(d1$V1*d1$V4)

plot(fits[,1], log="y")

# fit the intercepts
cor(sqrt(log(fits[,1])), ll)
fit = lm(sqrt(log(fits[,1])) ~ ll)
plot(ll, sqrt(log(fits[,1])))
abline(fit)
title("fitted intercepts")

# fit the slope
cor(sqrt(log(fits[,2])-log(fits[1,2])), ll)
fit = lm(sqrt(log(fits[,2])-log(fits[1,2])) ~ ll)
plot(ll, sqrt(log(fits[,2])-log(fits[1,2])))
abline(fit)
title("fitted slopes")








