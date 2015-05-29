import math
import numpy as np

def deviation_raw(x, y):
    """
    Quick and dirty absolute error
    """
    return [math.fabs(xi-yi) for xi, yi in zip(x, y)]

def deviation_normalized(x, y):
    """
    Quick and dirty normalized rms error for now
    """
    return [(xi-yi)*(xi-yi)/(xi*yi) for xi, yi in zip(x, y)]

def deviation(x, y):
    """
    Attempt to return a reasonable deviation.  When the data is normalized
    correctly, we shouldn't need this
    """
    if all([i >= 1.0 for i in x]):
        return deviation_normalized(x, y)
    else:
        return deviation_raw(x, y)

def detrend(df):
    """
    Takes a pandas dataframe and attempts to detrend

    ** Note ** this is just a placeholder for an actual detrending
    function to be filled in later.
    """
    y0 = df[df.columns[0]]
    unit = np.array([1.0 for i in y0])
    y = []
    delta = []
    # polynomial fit (overfit for now)
    x = range(len(y0))
    p = np.polyfit(x, y0, 6)
    poly = np.poly1d(p)
    yp = [poly(i) for i in x]
    y.append(yp)
    delta.append(deviation(y0, yp))
    # fft fit
    rft = np.fft.rfft(y0)
    for i in range(0, len(rft)):
        if math.fabs(rft[i]) < np.mean(y0):
            rft[i] = 0.0
    yfft = np.fft.irfft(rft, n=len(y0))
    y.append(yfft)
    delta.append(deviation(y0, yfft))
    # fit polynomial to wow and yoy growth rates
    # extrapolate each data point from previous week and year
    dd = [sum(d) for d in delta]
    idx = np.argmin(dd)
    df['Trend'] = y[idx]
    df['Residuals'] = delta[idx]
    return df
