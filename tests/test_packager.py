import zipfile

from tests import air_mock
import pytest

from pit.packager import create_package


@pytest.mark.asyncio
async def test_create_package_returns_archive(thesis_1, thesis_1_pdf, thesis_2,
                                              thesis_2_pdf):
    with air_mock.Mock() as m:
        m.get('mock://example.com/docset',
              json={'members': ['mock://example.com/theses/1',
                                'mock://example.com/theses/2']})
        m.get('mock://example.com/theses/1', text=thesis_1)
        m.get('mock://example.com/theses/1/1.pdf', content=thesis_1_pdf)
        m.get('mock://example.com/theses/2', text=thesis_2)
        m.get('mock://example.com/theses/2/2.pdf', content=thesis_2_pdf)
        pkg = await create_package('mock://example.com/docset')
        with zipfile.ZipFile(pkg) as zf:
            members = zf.namelist()
        assert '1.pdf' in members
        assert '2.pdf' in members
