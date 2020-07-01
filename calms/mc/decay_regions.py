from typing import Tuple, Dict
import asyncio

from hep_tables import xaod_table
from hl_tables.atlas import a_3v
from hl_tables import count_async
import matplotlib.patches as patches
import matplotlib.pyplot as plt


class detector_range:
    def __init__(self, name: str, range: Tuple[float, float], eff: float):
        self._name = name
        self._x_min = range[0]
        self._x_max = range[1]
        self._eff = eff

    def draw_lxy_box(self):
        x_center = (self._x_max - self._x_min)/2 + self._x_min

        axes = plt.gca()
        y_min = axes.get_ylim()[0]
        y_max = axes.get_ylim()[1] * 0.90
        y_center = (y_max - y_min)/2 + y_min

        plt.text(x_center, y_center, self._name, size=10, ha="center", va="center", color='w')

        r = patches.Rectangle((self._x_min, y_min), self._x_max - self._x_min, y_max-y_min,
                              linewidth=1, edgecolor='r', facecolor='r', alpha=0.5)
        plt.gca().add_patch(r)
        axes.add_patch(r)

    def inside(self, lxy):
        'Return a max of lxy if lxy is in the range'
        return (lxy > self._x_min) & (lxy <= self._x_max)


# Define a region for the calorimeter and the muon spectrometer.
# These positions were pulled from the sensitivity curves of the initial
# Run 2 analyses - and are meant to be only approximate.
region_cal = detector_range('cal', (2.0, 3.5), 0.3)
region_muon = detector_range('muon', (4.0, 7.0), 0.3)


async def analysis_counts(s: xaod_table) -> Dict[str, int]:
    '''
    For a given sample containing long lived particles, return the number that decay in:

    1. Total number of good events that we are looking at
    1. At least one in the muon spectrometer
    1. At least one in the calorimeter
    1. 2 in the muon spectrometer
    1. 2 in the calorimeter
    1. 1 in the muon spectrometer, and 1 in the calorimeter.

    We assume:
        - The pdgID is 35
        - That there are no more than 2 long lived scalars in the event.

    We do check to make sure both long lived particles have decayed - something the MC
    doesn't always do.
    '''
    truth = s.TruthParticles('TruthParticles')
    llp_truth = truth[truth.pdgId == 35]
    llp_good_truth = llp_truth[llp_truth.hasProdVtx & llp_truth.hasDecayVtx]
    l_prod = a_3v(llp_good_truth.prodVtx)
    l_decay = a_3v(llp_good_truth.decayVtx)
    lxy = (l_decay-l_prod).xy/1000.0
    lxy_2 = lxy[lxy.Count() == 2]

    a_total = count_async(lxy_2)
    a_has_1muon = count_async(lxy_2[lxy_2.mapseq(lambda s: region_muon.inside(s[0])
                                    | region_muon.inside(s[1]))])
    a_has_1cal = count_async(lxy_2[lxy_2.mapseq(lambda s: region_cal.inside(s[0])
                                   | region_cal.inside(s[1]))])
    a_has_2muon = count_async(lxy_2[lxy_2.mapseq(lambda s: region_muon.inside(s[0])
                                    & region_muon.inside(s[1]))])
    a_has_2cal = count_async(lxy_2[lxy_2.mapseq(lambda s: region_cal.inside(s[0])
                                   & region_cal.inside(s[1]))])
    a_has_calms = count_async(lxy_2[lxy_2.mapseq(lambda s: (region_cal.inside(s[0])
                                                            & region_muon.inside(s[1]))
                                    | (region_muon.inside(s[0])
                                       & region_cal.inside(s[1])))])

    n_total, n_has_1muon, n_has_1cal, n_has_2muon, n_has_2cal, n_has_calms = \
        await asyncio.gather(*[a_total, a_has_1muon, a_has_1cal,
                               a_has_2muon, a_has_2cal, a_has_calms])

    return dict(
        total=n_total,
        has_1muon=n_has_1muon,
        has_1cal=n_has_1cal,
        has_2muon=n_has_2muon,
        has_2cal=n_has_2cal,
        has_calms=n_has_calms
    )
