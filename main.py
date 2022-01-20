from calc.stats import Stats, PlotFig
import luigi


if __name__ == '__main__':
    # luigi.build([Stats()], local_scheduler=True)
    # Parameters are configured in config.py
    luigi.build([PlotFig()], local_scheduler=True)
