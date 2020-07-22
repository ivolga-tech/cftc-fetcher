#! /usr/bin/env python3


# paj-fetcher -- Download and convert data from PAJ
# By: Evgeniy Smirnov <rassouljb@gmail.com>
#
# Copyright (C) 2020 Cepremap
# https://git.nomics.world/dbnomics-fetchers/paj-fetcher
#
# paj-fetcher is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# paj-fetcher is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Download data from PAJ, write it in a target directory.

See also `.gitlab-ci.yml` in which data is committed to a Git repository of source data.
"""

import argparse
import asyncio
import http.client
import logging
import re
import shutil
import requests

from pathlib import Path
from typing import AsyncIterator, Awaitable
from aiohttp import ClientSession
from dbnomics_fetcher_toolbox.arguments import add_arguments_for_download
from dbnomics_fetcher_toolbox.formats import fetch_or_read_xml, \
    fetch_xml, HTML_PARSER
from dbnomics_fetcher_toolbox.logging_utils import setup_logging
from dbnomics_fetcher_toolbox.resources import Resource, process_resources
from dbnomics_fetcher_toolbox.status import load_events, open_status_writer

log = logging.getLogger(__name__)

PAJ_URL = 'https://www.paj.gr.jp'
DATASETS_HOST_PAGE = PAJ_URL + '/english/statis/'


class PAJResource(Resource):
    dir: Path
    url: str
    name: str

    def create_context(self):
        self.dir.mkdir(exist_ok=True, parents=True)

    def delete(self):
        """Delete HTML file and all Excel files."""
        shutil.rmtree(self.dir)


async def prepare_resources(target_dir: Path) -> AsyncIterator[PAJResource]:
    # datasets informations
    log.info("Downloading files from start page...")
    session = ClientSession()
    start_page = await fetch_xml(DATASETS_HOST_PAGE, session=session,
                                 parser=HTML_PARSER)
    for list_item in start_page.findall('.//{*}ul[@class="icon_list"]/li'):
        file_link = list_item.find('a')
        if '[xls]' in file_link.text:
            update_date = re.search(r"\((.*?) .*\)", list_item.find('span').text) \
                .group(1).replace('/', '-')
            (index, filename) = file_link.get('href') \
                .replace("/english/statis/data/", "") \
                .split('/')
            yield PAJResource(id=update_date + '_' + filename.split('.')[0],
                              dir=target_dir / index,
                              url=PAJ_URL + file_link.get('href'),
                              name=update_date + '_' + filename)
    await session.close()


def process_resource(resource: PAJResource):
    log.info("Download file %s to input_dir %s" % (resource.url, resource.dir))
    resp = requests.get(resource.url, stream=True)
    resp.raise_for_status()
    with (resource.dir / resource.name).open("wb") as _file:
        for chunk in resp.iter_content(chunk_size=512):
            if chunk:
                _file.write(chunk)


async def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--debug-http', action='store_true',
                        help='display http.client debug messages')
    parser.add_argument('--log', default='WARNING', help='level of logging messages')
    add_arguments_for_download(parser)
    args = parser.parse_args()

    setup_logging(args)
    logging.getLogger("urllib3").setLevel(
        logging.DEBUG if args.debug_http else logging.WARNING)
    if args.debug_http:
        http.client.HTTPConnection.debuglevel = 1

    target_dir = args.target_dir
    if not target_dir.exists():
        parser.error("Target input_dir {!r} not found".format(str(target_dir)))

    resources = [r async for r in prepare_resources(target_dir)]
    events = load_events(target_dir)

    with open_status_writer(args) as append_event:
        await process_resources(
            resources=resources,
            args=args,
            process_resource=process_resource,
            on_event=append_event,
            events=events,
        )

    return 0


if __name__ == '__main__':
    asyncio.run(main())
