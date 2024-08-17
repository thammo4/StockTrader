# FILE: `edgar10Q.py`
from config import *


# cik_lookup = {'AAPL': '0000320193', 'MSFT': '0000789019', 'AMZN': '0001018724', 'GOOGL': '0001652044', 'META': '0001326801', 'TSLA': '0001318605', 'BRK.B': '0001067983', 'JNJ': '0000200406', 'JPM': '0000019617', 'V': '0001403161', 'PG': '0000732717', 'KO': '0000066740', 'PEP': '0000354950', 'XOM': '0000078003', 'NFLX': '0000310158', 'DIS': '0000909832', 'NVDA': '0001045810', 'INTC': '0000050863', 'IBM': '0000051143', 'CSCO': '0000789010', 'GE': '0000021344', 'BA': '0000092122', 'MCD': '0000208692', 'COST': '0000314201', 'MRK': '0000726728', 'ORCL': '0001065280', 'T': '0001104659', 'PYPL': '0001356576', 'ADBE': '0001551152', 'UBER': '0001730168', 'ZM': '0001755672', 'SBUX': '0000906280', 'AMD': '0000860730', 'QCOM': '0001108524', 'HON': '0000783280', 'MDT': '0000203577', 'MMM': '0000064803', 'CAT': '0000072971', 'CVX': '0000029989', 'AXP': '0000093751', 'GS': '0000077476', 'FDX': '0000059478', 'TGT': '0000004904', 'BK': '0000097745', 'C': '0000072333', 'LLY': '0000723254', 'HPQ': '0000320187', 'WMT': '0000006201', 'AMAT': '0001011006', 'ISRG': '0001090872'}


#
# Map: symbol -> CIK
#

def get_cik (symbol):
	return cik_lookup.get(symbol, None);


#
# Map: CIK -> Metadata list
#

def get_filing_metadata (cik):
	url_metadata = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json";
	r = requests.get(
		url=url_metadata,
		headers = {'User-Agent': 'tom hammons qje5vf@virginia.edu', 'Accept-Encoding':'gzip, deflate'}
	);
	return r.json()['filings']['recent'];


#
# Map: Metadatalist -> 10Q Filings DF
#

def filter_filings (filings, form_type='10-Q'):
	df_filings = pd.DataFrame(filings);
	df_filtered = df_filings[['accessionNumber', 'filingDate', 'form', 'fileNumber', 'items', 'size']];
	df_filtered.rename(columns={'accessionNumber':'accession_number', 'filingDate':'filing_date', 'fileNumber':'file_number'}, inplace=True);

	return df_filtered.loc[df_filtered['form'] == form_type];


#
# Map: DF(Filings) -> Single most recent filing
#

def get_latest_filing (df_filtered):
	return df_filtered.iloc[0];


def get_filing_content (cik, accession_number):
	url_10q = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}.txt"
	r = requests.get(
		url = url_10q,
		headers = {'User-Agent':'qje5vf@virginia.edu'}
	);
	return r.text;


def parse_filing (html_str):
	xbrl_initial = html_str.find("<XBRL>");
	xbrl_final = html_str.find("</XBRL>") + len("</XBRL>");
	xbrl_content = html_str[xbrl_initial:xbrl_final];

	xml_initial = xbrl_content.find("<?xml");
	xml_content = xbrl_content[xml_initial:];

	root = etree.fromstring(xml_content.encode('ASCII'), etree.XMLParser(recover=True));

	ns_xml = dict(root.nsmap.items());
	ns_xbrli, ns_ix = [ns_xml['xbrli'], ns_xml['ix']];

	elems = root.findall(f".//{{{ns_ix}}}nonFraction");

	gaap_data = [];
	for x in elems:
		val = None;
		if x.text:
			try:
				val = float(x.text.replace(',', ''));
			except ValueError:
				print(f"ISSUE: [{x.get('name')}]: float converstion x.text='{x.text}'");

		gaap_data.append({
			'name': x.get('name'),
			'context': x.get('contextRef'),
			'decimals': x.get('decimals'),
			'units': x.get('unitRef'),
			'scale': x.get('scale'),
			'format': x.get('format'),
			'value': val
		});

	return gaap_data;

def filing_df (gaap_list):
	df_gaap = pd.DataFrame(gaap_list);
	df_gaap[['prefix', 'field']] = df_gaap['name'].str.split(':', n=1, expand=True);
	df_gaap = df_gaap[['prefix', 'field', 'context', 'format', 'decimals', 'units', 'scale', 'value']];

	return df_gaap;




#
# Example - IBM
#

ibm_cik = get_cik('IBM');
ibm_metadata = get_filing_metadata(ibm_cik);
df_ibm = filter_filings(ibm_metadata);
ibm_latest = get_latest_filing(df_ibm);
ibm_html = get_filing_content(ibm_cik, ibm_latest['accession_number']);
# ibm_elems = parse_filing(ibm_html);
ibm_gaap = parse_filing(ibm_html);

df_gaap = filing_df(ibm_gaap);