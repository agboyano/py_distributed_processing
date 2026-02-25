import datetime
import secrets
from math import exp, sqrt
import numpy as np
from numpy.random import PCG64, Generator

# Multiprocessing con números aleatorios paralelos basados en RandomState
# RandomState se ha quedado anticuado y utilza el generador MT19937
# que es más lento que el generador ahora por defecto en numpy PGC64 


def new_price(S_t, v, r, t1, t2):
    T = (t2 - t1) / 365.0
    # no hay diferencia con gauss
    return S_t * exp((r - 0.5 * v * v) * T + v * sqrt(T) * np.random.randn())


def df(r, t1, t2):
    return exp(-r * (t2 - t1) / 365.0)


# Multiprocessing con números aleatorios paralelos basados en RandomState
# RandomState se ha quedado anticuado y además utilza el generador MT19937
# que es más lento que el generador ahora por defecto en numpy PGC64 

def mc_autocall_mapper(nsimulations,
                       v,
                       r,
                       coupon_barrier,
                       kickout_barrier,
                       protection_barrier,
                       coupon_rate,
                       dates):

    assert(coupon_barrier >= protection_barrier)

    # Hay que tener cuidado con la desserialización desde json.
    # En json son todo flotantes.
    nsimulations = int(nsimulations)

    S = 1.0
    C = 1000.0

    start_date = dates[0]

    autocall_prices = [0.0] * nsimulations

    coupons_discounted = [C * coupon_rate * df(r, start_date, dates[j]) for j in range(len(dates))]
    principal_discounted = [C * df(r, start_date, dates[j]) for j in range(len(dates))]

    def p_exp(t1, t2):
        T = (t2 - t1) / 365.0
        return ((r - 0.5 * v**2) * T, v * sqrt(T))

    partial_exp = [p_exp(dates[j], dates[j+1]) for j in range(len(dates)-1)]

    rnd = np.random.RandomState().randn(nsimulations, len(dates)-1)

    # muy importante RandomState() para multiproceso ya que reinicializa la semilla
    # numpy.random.RandomState(seed=None)
    # If seed is None, then the MT19937 BitGenerator is initialized by reading data from /dev/urandom
    # (or the Windows analogue) if available or seed from the clock otherwise.

    for i in range(nsimulations):
        autocall_price = 0.0

        for j in range(1, len(dates)):
            S = S * exp(partial_exp[j-1][0] + partial_exp[j-1][1] * rnd[i, j-1])

            if S > coupon_barrier:
                autocall_price += coupons_discounted[j]

            if S >= kickout_barrier:
                break

        if S < protection_barrier:
            autocall_price += S * principal_discounted[j]

        else:
            autocall_price += principal_discounted[j]

        autocall_prices[i] = autocall_price

    return sum(autocall_prices), float(nsimulations)


NSIMULATIONS = 1000000

V = 0.20  # Volatilidad
R = 0.04  # Interés instantáneo (ln(1+r)?)
COUPON_BARRIER = 0.8
KICKOUT_BARRIER = 1.1
PROTECTION_BARRIER = 0.6
COUPON_RATE = 0.088

obs_dates = [datetime.datetime(2012, 7, 4, 0, 0),
             datetime.datetime(2013, 7, 5, 0, 0),
             datetime.datetime(2014, 7, 7, 0, 0),
             datetime.datetime(2015, 7, 6, 0, 0),
             datetime.datetime(2016, 7, 5, 0, 0),
             datetime.datetime(2017, 7, 5, 0, 0),
             datetime.datetime(2018, 7, 5, 0, 0)]

dates = [int((t-obs_dates[0]).days) for t in obs_dates]

# Para pasar a multiprocessing. Si no da problemas en los
# notebooks de Jupyter en windows.


def f(nsimulations):
    return mc_autocall_mapper(nsimulations,
                              v=V,
                              r=R,
                              coupon_barrier=COUPON_BARRIER,
                              kickout_barrier=KICKOUT_BARRIER,
                              protection_barrier=PROTECTION_BARRIER,
                              coupon_rate=COUPON_RATE,
                              dates=dates)


# Versión basada en PGC64. Hay que pasar a la función la semilla y el número de salto.
# Basado en https://numpy.org/doc/1.18/reference/random/parallel.html

def mc_autocall_mapper2(rndg, nsimulations,
                   v=V,
                   r=R, 
                   coupon_barrier=COUPON_BARRIER,
                   kickout_barrier=KICKOUT_BARRIER,
                   protection_barrier=PROTECTION_BARRIER,
                   coupon_rate=COUPON_RATE,
                   dates=dates):
    

    rng = Generator(PCG64(int(rndg[0])).jumped(int(rndg[1])))
    
    assert(coupon_barrier >= protection_barrier)

    # Hay que tener cuidado con la desserialización desde json. 
    # En json son todo flotantes.
    nsimulations = int(nsimulations)
    
    S = 1.0
    C = 1000.0

    start_date = dates[0]

    autocall_prices = [0.0] * nsimulations

    coupons_discounted = [C * coupon_rate * df(r, start_date, dates[j]) for j in range(len(dates))]
    principal_discounted = [C * df(r, start_date, dates[j]) for j in range(len(dates))]
    
    def p_exp(t1, t2):
        T = (t2 - t1) / 365.0
        return ((r - 0.5 * v**2) * T, v * sqrt(T)) 
        
    
    partial_exp = [p_exp(dates[j], dates[j+1]) for j in range(len(dates)-1)]
    
    rnd = rng.standard_normal((nsimulations, len(dates)-1)) 
    
    
    for i in range(nsimulations):
        autocall_price = 0.0
        
        for j in range(1, len(dates)):
            S = S * exp(partial_exp[j-1][0] + partial_exp[j-1][1] * rnd[i, j-1]) 
    
            if S > coupon_barrier:
                autocall_price += coupons_discounted[j]
                
            if S >= kickout_barrier:
                break

        if S < protection_barrier:
            autocall_price += S * principal_discounted[j]
                
        else:
            autocall_price += principal_discounted[j]

        autocall_prices[i] = autocall_price

    return sum(autocall_prices), float(nsimulations)

def f2(x):
    seed = int(x[0])
    jump = int(x[1])
    nsim = int(x[2])
    return mc_autocall_mapper2(rndg=(seed, jump),
                                nsimulations=nsim,
                                v=V,
                                r=R, 
                                coupon_barrier=COUPON_BARRIER,
                                kickout_barrier=KICKOUT_BARRIER,
                                protection_barrier=PROTECTION_BARRIER,
                                coupon_rate=COUPON_RATE,
                                dates=dates)


def mc_autocall_empty_mapper(rndg, nsimulations,
                   v=V,
                   r=R, 
                   coupon_barrier=COUPON_BARRIER,
                   kickout_barrier=KICKOUT_BARRIER,
                   protection_barrier=PROTECTION_BARRIER,
                   coupon_rate=COUPON_RATE,
                   dates=dates):
    

    return 1045.0, float(nsimulations)
    