#!/usr/bin/env python3

import typing as typ
from dataclasses import dataclass
from itertools import islice
from time import sleep, time_ns

import bs4 # type: ignore
import requests


POLL_SEC = 5*60
STATUS_URL = 'http://192.168.100.1/cmSignalData.htm'


InfluxSet = typ.Mapping[str, typ.Any]

@dataclass
class InfluxPoint:
    measurement: str
    tag_set: InfluxSet
    field_set: InfluxSet
    timestamp: int

    def linep(self) -> str:
        def strset(s: InfluxSet) -> str:
            return ','.join(f'{k}={strval(v)}' for k, v in s.items())
        def strval(v: typ.Any) -> str:
            if type(v) is str:
                s = v.replace('"', '\\"')
                s = s.replace('\n', '\\n')
                return f'"{s}"'
            else:
                return str(v)

        return (f'{self.measurement},{strset(self.tag_set)} '
                + f'{strset(self.field_set)} {self.timestamp}')


def main() -> None:
    while True:
        try:
            points = list(scrape())
        except:
            pass
        else:
            for pt in points:
                print(pt.linep())
        sleep(POLL_SEC)


def scrape() -> typ.Generator[InfluxPoint, None, None]:
    ts = time_ns()
    with requests.get(STATUS_URL) as req:
        bs = bs4.BeautifulSoup(req.text, 'html.parser')

    downstream, upstream, downstream_codewords = \
            [c.find('table') for c in bs('center')]
    yield from downstream_points(ts, downstream, downstream_codewords)
    yield from upstream_points(ts, upstream)


last_correct: typ.Dict[int, int] = {}
last_uncorrect: typ.Dict[int, int] = {}

def downstream_points(ts: int, table1: bs4.element.Tag, table2: bs4.element.Tag) \
        -> typ.Generator[InfluxPoint, None, None]:
    global last_correct, last_uncorrect

    # Read upper table with power levels.
    _, channel, frequency, snr, modulation, power = \
            table1.find('tbody')('tr', recursive=False)

    channel_ids = [int(td.text) for td in datacells(channel)]
    snrs = [float(td.text.split(' ')[0]) for td in datacells(snr)]
    modulations = [td.text.strip() for td in datacells(modulation)]
    power_levels = [float(td.text.split(' ')[0]) for td in datacells(power)]

    # Read lower table with codeword counts.
    _, _, _, correctable, uncorrectable = table2.find('tbody')('tr', recursive=False)

    correctables = [int(td.text) for td in datacells(correctable)]
    int_correctables = [v - last_correct.get(channel_ids[i], v)
                        for i, v in enumerate(correctables)]
    uncorrectables = [int(td.text) for td in datacells(uncorrectable)]
    int_uncorrectables = [v - last_uncorrect.get(channel_ids[i], v)
                          for i, v in enumerate(uncorrectables)]

    last_correct = {channel_ids[i]: v for i, v in enumerate(correctables)}
    last_uncorrect = {channel_ids[i]: v for i, v in enumerate(uncorrectables)}

    def field_set(i: int) -> InfluxSet:
        return { 'snr_db': snrs[i],
                 'modulation': modulations[i],
                 'power_dbmv': power_levels[i],
                 'interval_correctable_codewords': int_correctables[i],
                 'interval_uncorrectable_codewords': int_uncorrectables[i] }
    yield from (InfluxPoint(measurement='downstream',
                            tag_set={ 'channel_id': cid },
                            field_set=field_set(i),
                            timestamp=ts) for i, cid in enumerate(channel_ids))


def upstream_points(ts: int, table: bs4.element.Tag) \
        -> typ.Generator[InfluxPoint, None, None]:
    _, channel, frequency, service_id, symbol_rate, power, modulation, ranging = \
            table.find('tbody')('tr', recursive=False)

    channel_ids = [int(td.text) for td in datacells(channel)]
    power_levels = [float(td.text.split(' ')[0]) for td in datacells(power)]
    modulations = [td.text.strip() for td in datacells(modulation)]

    yield from (InfluxPoint(measurement='upstream',
                            tag_set={ 'channel_id': cid },
                            field_set={ 'power_dbmv': power_levels[i],
                                        'modulation': modulations[i] },
                            timestamp=ts) for i, cid in enumerate(channel_ids))


def datacells(tr: bs4.element.Tag) -> typ.Iterable[bs4.element.Tag]:
    return islice(tr('td', recursive=False), 1, None)


if __name__ == '__main__':
    main()

