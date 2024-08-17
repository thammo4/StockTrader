# FILE: `StockTrader/xom10Q_example.py`
from config import * # imports necessary modules and variables

#
# Example Using SEC EDGAR API To Retrieve Apple SEC Filings
#

BASE_SEC_URL = 'https://data.sec.gov';
SEC_HEADER = {'User-Agent':'tom hammons qje5vf@virginia.edu', 'Accept-Encoding':'gzip, deflate'};

cik_lookup = {
	'AAPL': '0000320193',
	'XOM': '34088'
};


#
# Retrieve SEC Filing Metadata for Exxon Mobil
#


# FILINGS:
#           accessionNumber  filingDate  reportDate        acceptanceDateTime act      form  ...      items     size isXBRL  isInlineXBRL           primaryDocument  primaryDocDescription
# 0    0000034088-24-000050  2024-08-05  2024-06-30  2024-08-05T13:15:44.000Z  34      10-Q  ...             8451717      1             1          xom-20240630.htm              FORM 10-Q
# 1    0000034088-24-000048  2024-08-02  2024-08-02  2024-08-02T06:31:19.000Z  34       8-K  ...  2.02,7.01  1797462      1             1          xom-20240802.htm               FORM 8-K
# 2    0000034088-24-000045  2024-08-01  2024-05-03  2024-08-01T09:46:48.000Z  34     8-K/A  ...       5.02   220429      1             1          xom-20240503.htm             FORM 8-K/A
# 3    0000034088-24-000042  2024-07-08  2024-07-08  2024-07-08T07:07:33.000Z  34       8-K  ...       7.01   264546      1             1          xom-20240708.htm               FORM 8-K
# 4    0001127602-24-019484  2024-07-01  2024-07-01  2024-07-01T12:59:11.000Z             3  ...                6268      0             0      xslF345X02/form3.xml       PRIMARY DOCUMENT
# ..                    ...         ...         ...                       ...  ..       ...  ...        ...      ...    ...           ...                       ...                    ...
# 995  0000034088-18-000012  2018-02-09  2018-02-08  2018-02-09T15:24:11.000Z  34       8-K  ...  2.02,7.01    82743      0             0             r8k020918.htm               FORM 8-K
# 996  0000932471-18-003753  2018-02-09              2018-02-09T10:46:23.000Z  34  SC 13G/A  ...               45351      0             0        exxonmobilcorp.htm                       
# 997  0000034088-18-000010  2018-02-08              2018-02-08T16:07:58.000Z      SC 13G/A  ...                6690      0             0  xom13g123117imperial.txt                       
# 998  0001127602-18-004534  2018-02-08  2018-02-06  2018-02-08T14:25:00.000Z             4  ...               21370      0             0      xslF345X03/form4.xml       PRIMARY DOCUMENT
# 999  0000215457-18-005185  2018-02-08              2018-02-08T09:47:35.000Z  34  SC 13G/A  ...               13808      0             0   us30231g1022_012418.txt                       

# [1000 rows x 14 columns]
url_metadata = f"{BASE_SEC_URL}/submissions/CIK{cik_lookup['XOM'].zfill(10)}.json";
r = requests.get(
	url = url_metadata,
	headers = {'User-Agent':'tom hammons qje5vf@virginia.edu', 'Accept-Encoding':'gzip, deflate'});

filings = r.json()['filings']['recent'];
df_filings = pd.DataFrame(filings);
print(f'FILINGS:\n{df_filings}');


# FILTERED FILINGS:
#          accession_number filing_date      form file_number      items     size
# 0    0000034088-24-000050  2024-08-05      10-Q   001-02256             8451717
# 1    0000034088-24-000048  2024-08-02       8-K   001-02256  2.02,7.01  1797462
# 2    0000034088-24-000045  2024-08-01     8-K/A   001-02256       5.02   220429
# 3    0000034088-24-000042  2024-07-08       8-K   001-02256       7.01   264546
# 4    0001127602-24-019484  2024-07-01         3                            6268
# ..                    ...         ...       ...         ...        ...      ...
# 995  0000034088-18-000012  2018-02-09       8-K   001-02256  2.02,7.01    82743
# 996  0000932471-18-003753  2018-02-09  SC 13G/A   005-81094               45351
# 997  0000034088-18-000010  2018-02-08  SC 13G/A                            6690
# 998  0001127602-18-004534  2018-02-08         4                           21370
# 999  0000215457-18-005185  2018-02-08  SC 13G/A   005-81094               13808

# [1000 rows x 6 columns]
filings_filtered = df_filings[['accessionNumber', 'filingDate', 'form', 'fileNumber', 'items', 'size']];
filings_filtered.rename(columns={'accessionNumber':'accession_number', 'filingDate':'filing_date', 'fileNumber':'file_number'}, inplace=True);
print(f"FILTERED FILINGS:\n{filings_filtered}");



#
# Retrieve Form 10-Q Data
#

# 10-Qs
#          accession_number filing_date  form file_number items      size
# 0    0000034088-24-000050  2024-08-05  10-Q   001-02256         8451717
# 42   0000034088-24-000029  2024-04-29  10-Q   001-02256         6149650
# 117  0000034088-23-000056  2023-10-31  10-Q   001-02256         7483414
# 149  0000034088-23-000048  2023-08-01  10-Q   001-02256         7198721
# 184  0000034088-23-000030  2023-05-02  10-Q   001-02256         6154856
# 255  0000034088-22-000064  2022-11-02  10-Q   001-02256         7359169
# 264  0000034088-22-000051  2022-08-03  10-Q   001-02256         7238532
# 280  0000034088-22-000026  2022-05-04  10-Q   001-02256         5502693
# 363  0000034088-21-000064  2021-11-03  10-Q   001-02256         6296884
# 372  0000034088-21-000051  2021-08-04  10-Q   001-02256         6227874
# 445  0000034088-21-000024  2021-05-05  10-Q   001-02256         5464349
# 743  0000034088-20-000090  2020-11-04  10-Q   001-02256         6415560
# 750  0000034088-20-000073  2020-08-05  10-Q   001-02256        10321112
# 770  0000034088-20-000037  2020-05-06  10-Q   001-02256         8345288
# 852  0000034088-19-000062  2019-11-06  10-Q   001-02256        10972306
# 859  0000034088-19-000042  2019-08-07  10-Q   001-02256        10771871
# 874  0000034088-19-000017  2019-05-02  10-Q   001-02256         7459102
# 962  0000034088-18-000048  2018-11-07  10-Q   001-02256         6729656
# 965  0000034088-18-000039  2018-08-02  10-Q   001-02256         6660774
# 980  0000034088-18-000024  2018-05-03  10-Q   001-02256         5274401
filings_10q = filings_filtered.loc[filings_filtered['form'] == '10-Q'];
print(f'10-Qs\n{filings_10q}');


# LAST FILE:
# accession_number    0000034088-24-000050
# filing_date                   2024-08-05
# form                                10-Q
# file_number                    001-02256
# items                                   
# size                             8451717
# Name: 0, dtype: object
last_filing = filings_10q.iloc[0];
print(f'LAST FILE:\n{last_filing}');

url_10q = f"https://www.sec.gov/Archives/edgar/data/{cik_lookup['XOM']}/{last_filing['accession_number']}.txt";

r = requests.get(
	url = url_10q,
	headers = {'User-Agent': 'tom hammons qje5vf@virginia.edu'}
);


#
# Parse Form 10-Q Markup Response
#

xbrl_initial = r.text.find('<XBRL>');
xbrl_final = r.text.find('</XBRL>') + len('</XBRL>');

xbrl_content = r.text[xbrl_initial:xbrl_final];

xml_initial = xbrl_content.find('<?xml');
xml_content = xbrl_content[xml_initial:];

root = etree.fromstring(xml_content.encode('ASCII'), etree.XMLParser(recover=True));

#
# Determine namespaces present in the XML - iXBRL, XBRL (, HTML)
#

# >>> ns_xml
# {'xbrldi': 'http://xbrl.org/2006/xbrldi', 'xlink': 'http://www.w3.org/1999/xlink', 'xbrli': 'http://www.xbrl.org/2003/instance', None: 'http://www.w3.org/1999/xhtml', 'ixt': 'http://www.xbrl.org/inlineXBRL/transformation/2020-02-12', 'utr': 'http://www.xbrl.org/2009/utr', 'country': 'http://xbrl.sec.gov/country/2024', 'us-gaap': 'http://fasb.org/us-gaap/2024', 'iso4217': 'http://www.xbrl.org/2003/iso4217', 'ixt-sec': 'http://www.sec.gov/inlineXBRL/transformation/2015-08-31', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'link': 'http://www.xbrl.org/2003/linkbase', 'ix': 'http://www.xbrl.org/2013/inlineXBRL', 'srt': 'http://fasb.org/srt/2024', 'dei': 'http://xbrl.sec.gov/dei/2024', 'ecd': 'http://xbrl.sec.gov/ecd/2024', 'xom': 'http://www.exxonmobil.com/20240630'}

# >>> df_ns_xml = pd.DataFrame(list(ns_xml.items()), columns=['prefix', 'uri']);
# >>> df_ns_xml
#      prefix                                                uri
# 0    xbrldi                        http://xbrl.org/2006/xbrldi
# 1     xlink                       http://www.w3.org/1999/xlink
# 2     xbrli                  http://www.xbrl.org/2003/instance
# 3      None                       http://www.w3.org/1999/xhtml
# 4       ixt  http://www.xbrl.org/inlineXBRL/transformation/...
# 5       utr                       http://www.xbrl.org/2009/utr
# 6   country                   http://xbrl.sec.gov/country/2024
# 7   us-gaap                       http://fasb.org/us-gaap/2024
# 8   iso4217                   http://www.xbrl.org/2003/iso4217
# 9   ixt-sec  http://www.sec.gov/inlineXBRL/transformation/2...
# 10      xsi          http://www.w3.org/2001/XMLSchema-instance
# 11     link                  http://www.xbrl.org/2003/linkbase
# 12       ix                http://www.xbrl.org/2013/inlineXBRL
# 13      srt                           http://fasb.org/srt/2024
# 14      dei                       http://xbrl.sec.gov/dei/2024
# 15      ecd                       http://xbrl.sec.gov/ecd/2024
# 16      xom                 http://www.exxonmobil.com/20240630

ns_xml = {};
for prefix, uri in root.nsmap.items():
	ns_xml[prefix] = uri;

# (xbrli, ix) = (XBRL, Inline XBRL)
ns_xbrli, ns_ix = [ns_xml['xbrli'], ns_xml['ix']];


#
# Financial data contained in iXBRL tags
# iXBRL tags are contained in HTML tags
#

elems = root.findall(f".//{{{ns_ix}}}nonFraction")

gaap_data = [];
for x in elems:
	val = None;
	if x.text:
		try:
			val = float(x.text.replace(',', ''));
		except ValueError:
			print(f"ISSUE [{x.get('name')}]: float conversion x.text='{x.text}'");
	gaap_data.append({
		'name': x.get('name'),
		'context': x.get('contextRef'),
		'decimals': x.get('decimals'),
		'units': x.get('unitRef'),
		'scale': x.get('scale'),
		'format': x.get('format'),
		'value': val
	});



# >>> df_gaap_data
#                                                   name context decimals        units scale               format         value
# 0                        us-gaap:CommonStockNoParValue     c-6     None  usdPerShare  None                 None           NaN
# 1                        us-gaap:CommonStockNoParValue    c-22     None  usdPerShare  None                 None           NaN
# 2               dei:EntityCommonStockSharesOutstanding     c-6      INF       shares     0  ixt:num-dot-decimal  4.442827e+09
# 3                                     us-gaap:Revenues     c-7       -6          usd     6  ixt:num-dot-decimal  8.998600e+04
# 4                                     us-gaap:Revenues     c-8       -6          usd     6  ixt:num-dot-decimal  8.079500e+04
# ..                                                 ...     ...      ...          ...   ...                  ...           ...
# 914                                   us-gaap:Revenues    c-10       -6          usd     6  ixt:num-dot-decimal  1.644390e+05
# 915         us-gaap:ProceedsFromSaleOfProductiveAssets   c-273       -8          usd     9  ixt:num-dot-decimal  1.600000e+00
# 916  xom:DisposalGroupNotDiscontinuedOperationGainL...   c-273       -8          usd     9  ixt:num-dot-decimal  4.000000e-01
# 917         us-gaap:ProceedsFromSaleOfProductiveAssets   c-274       -8          usd     9  ixt:num-dot-decimal  4.100000e+00
# 918  xom:DisposalGroupNotDiscontinuedOperationGainL...   c-274       -8          usd     9  ixt:num-dot-decimal  6.000000e-01

# [919 rows x 7 columns]
df_gaap_data = pd.DataFrame(gaap_data);
print(f"df_gaap_data\n{df_gaap_data}");


#
# Separate the `name` column into `prefix` and `field` per colon
#

# >>> df_gaap_data
#       prefix                                              field context decimals        units scale               format         value
# 0    us-gaap                              CommonStockNoParValue     c-6     None  usdPerShare  None                 None           NaN
# 1    us-gaap                              CommonStockNoParValue    c-22     None  usdPerShare  None                 None           NaN
# 2        dei                 EntityCommonStockSharesOutstanding     c-6      INF       shares     0  ixt:num-dot-decimal  4.442827e+09
# 3    us-gaap                                           Revenues     c-7       -6          usd     6  ixt:num-dot-decimal  8.998600e+04
# 4    us-gaap                                           Revenues     c-8       -6          usd     6  ixt:num-dot-decimal  8.079500e+04
# ..       ...                                                ...     ...      ...          ...   ...                  ...           ...
# 914  us-gaap                                           Revenues    c-10       -6          usd     6  ixt:num-dot-decimal  1.644390e+05
# 915  us-gaap                 ProceedsFromSaleOfProductiveAssets   c-273       -8          usd     9  ixt:num-dot-decimal  1.600000e+00
# 916      xom  DisposalGroupNotDiscontinuedOperationGainLossO...   c-273       -8          usd     9  ixt:num-dot-decimal  4.000000e-01
# 917  us-gaap                 ProceedsFromSaleOfProductiveAssets   c-274       -8          usd     9  ixt:num-dot-decimal  4.100000e+00
# 918      xom  DisposalGroupNotDiscontinuedOperationGainLossO...   c-274       -8          usd     9  ixt:num-dot-decimal  6.000000e-01

# [919 rows x 8 columns]
df_gaap_data[['prefix', 'field']] = df_gaap_data['name'].str.split(':', n=1, expand=True);
df_gaap_data = df_gaap_data[['prefix', 'field', 'context', 'decimals', 'units', 'scale', 'format', 'value']];

print('NO MAS');

