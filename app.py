import streamlit as st
import pandas as pd
import datetime
import re
import io
import traceback
import zipfile

st.set_page_config(page_title="GST Reconciliation Pro", layout="wide")

# ==========================================
# 🔐 THE GATEKEEPER (PASSWORD SYSTEM) 
# ==========================================
def check_password():
    """Returns True if the user entered the correct password."""
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.markdown("### 🔒 Secure Login Required")
        pwd = st.text_input("Enter the Master Password to access the Reconciliation Engine:", type="password")
        
        if pwd:
            # 🚨 Now securely pulls from secrets.toml or Streamlit Cloud Secrets 🚨
            if pwd == st.secrets["APP_PASSWORD"]: 
                st.session_state["password_correct"] = True
                st.rerun() 
            else:
                st.error("❌ Incorrect Password. Access Denied.")
        return False
    
    return True

# 🛑 STOP THE APP IF PASSWORD IS WRONG
if not check_password():
    st.stop()

with st.sidebar:
    if st.button("Logout 🚪"):
        st.session_state["password_correct"] = False
        st.rerun()

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def load_zoho_upload(uploaded_file):
    """Intelligently handles CSV, Excel, and ZIP files. Extracts, filters, and combines."""
    if uploaded_file.name.lower().endswith('.zip'):
        dfs = []
        with zipfile.ZipFile(uploaded_file, 'r') as z:
            for file_name in z.namelist():
                # Skip hidden system files and ensure it's a CSV
                if file_name.lower().endswith('.csv') and not file_name.startswith('__MACOSX'):
                    with z.open(file_name) as f:
                        temp_df = pd.read_csv(f)
                        
                        # EARLY MEMORY SAVER: Hunt for Status column and drop Drafts/Voids instantly
                        status_col = next((c for c in temp_df.columns if 'STATUS' in str(c).upper()), None)
                        if status_col:
                            temp_df = temp_df[~temp_df[status_col].astype(str).str.upper().isin(['DRAFT', 'VOID', 'CANCELLED'])]
                        
                        dfs.append(temp_df)
        
        if dfs:
            # Safely stitch all identical CSVs from the ZIP together
            return pd.concat(dfs, ignore_index=True)
        else:
            st.error("No valid CSV files found inside the uploaded ZIP.")
            return None
            
    elif uploaded_file.name.lower().endswith('.csv'):
        return pd.read_csv(uploaded_file)
    else:
        return pd.read_excel(uploaded_file)

def clean_match_key(series):
    """Safely converts to uppercase and removes non-alphanumeric chars without turning blanks to 'NAN'."""
    s = series.copy()
    s = s.fillna('').astype(str).str.strip().str.upper()
    s = s.replace(['NAN', 'NONE', 'NULL'], '')
    s = s.str.replace(r'[^A-Z0-9]', '', regex=True)
    return s

def process_gstr1(file, start_ts):
    """Phase 1-3: Ingest GSTR-1, Handle CDNR (-ve). NO DATE FILTERING ON GSTR-1."""
    xls = pd.ExcelFile(file)
    sheets = xls.sheet_names
    
    dfs = {}
    target_sheets = ['B2B', 'B2BA', 'EXP', 'EXPA', 'B2CL', 'B2CLA', 'B2CS', 'B2CSA', 'CDNR', 'CDNRA']
    
    for target in target_sheets:
        matched_sheet = None
        for s in sheets:
            if target.lower() == s.lower().strip():
                matched_sheet = s
                break
        
        if matched_sheet:
            df = pd.read_excel(xls, sheet_name=matched_sheet)
            
            # --- AUTO DETECT HEADER ROW ---
            col_str = ' '.join(df.columns.astype(str)).upper()
            if not any(x in col_str for x in ['INV', 'TAXABLE', 'RATE', 'ORIGINAL']):
                for i, row in df.head(15).iterrows():
                    row_str = ' '.join(row.astype(str)).upper()
                    if any(x in row_str for x in ['INV', 'TAXABLE', 'RATE', 'ORIGINAL']):
                        df.columns = row.values
                        df = df.iloc[i+1:].reset_index(drop=True)
                        break
            
            df = df.dropna(how='all')
            df.columns = df.columns.astype(str).str.strip().str.upper() 
            df = df.loc[:, ~df.columns.duplicated()]
            dfs[target] = df
        else:
            dfs[target] = pd.DataFrame() 

    def standardize_gstr(df, section_name, is_amendment=False):
        if df is None or df.empty: 
            return pd.DataFrame()
        
        col_map = {}
        mapped_targets = set() 
        
        for col in df.columns:
            c_up = str(col).upper()
            target = None
            
            # 🚨 HUNT FOR 'MONTH' OR 'PERIOD' IN ADDITION TO 'DATE' 🚨
            if 'ORIGINAL' in c_up and any(x in c_up for x in ['NO', 'NUMBER']): target = 'Original_Invoice_No'
            elif ('DATE' in c_up or 'MONTH' in c_up or 'PERIOD' in c_up) and 'ORIGINAL' not in c_up: target = 'Invoice_Date'
            elif any(x in c_up for x in ['INV', 'NOTE', 'VOUCHER', 'DOCUMENT']) and any(x in c_up for x in ['NO', 'NUMBER']): target = 'Invoice_No'
            elif 'TAXABLE' in c_up and 'VALUE' in c_up: target = 'Taxable_Value'
            elif c_up.strip() in ['RATE', 'RATE (%)', 'RATE(%)', 'RATE(PERCENTAGE)']: target = 'Rate'
            elif 'IGST' in c_up and 'RATE' not in c_up: target = 'IGST_Amount'
            elif 'CGST' in c_up and 'RATE' not in c_up: target = 'CGST_Amount'
            elif 'SGST' in c_up and 'RATE' not in c_up: target = 'SGST_Amount'
            elif 'REVERSE CHARGE' in c_up and 'TAX' not in c_up: target = 'Is_RCM'

            if target and target not in mapped_targets:
                col_map[col] = target
                mapped_targets.add(target)

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Safely assign dummy numbers to B2C sheets
        if 'Invoice_No' not in df.columns:
            if section_name in ['B2CS', 'B2CSA']:
                df['Invoice_No'] = f"{section_name}_SUMMARY_" + df.index.astype(str)
            else:
                return pd.DataFrame()
            
        if 'Is_RCM' in df.columns:
            df['GSTR_Is_RCM'] = df['Is_RCM'].astype(str).str.strip().str.upper().isin(['Y', 'YES'])
        else:
            df['GSTR_Is_RCM'] = False

        for tax in ['IGST_Amount', 'CGST_Amount', 'SGST_Amount']:
            if tax not in df.columns: df[tax] = 0.0
        if 'Taxable_Value' not in df.columns: df['Taxable_Value'] = 0.0
                
        df['Taxable_Value'] = pd.to_numeric(df['Taxable_Value'], errors='coerce').fillna(0)
        df['IGST_Amount'] = pd.to_numeric(df['IGST_Amount'], errors='coerce').fillna(0)
        df['CGST_Amount'] = pd.to_numeric(df['CGST_Amount'], errors='coerce').fillna(0)
        df['SGST_Amount'] = pd.to_numeric(df['SGST_Amount'], errors='coerce').fillna(0)
        
        # --- CREDIT NOTE NEGATION LOGIC (-ve) ---
        if section_name in ['CDNR', 'CDNRA']:
            doc_type_col = next((c for c in df.columns if 'DOCUMENT TYPE' in str(c).upper() or 'NOTE TYPE' in str(c).upper()), None)
            if doc_type_col:
                is_cn = df[doc_type_col].astype(str).str.upper().str.contains('C')
                df.loc[is_cn, 'Taxable_Value'] = -df.loc[is_cn, 'Taxable_Value'].abs()
                df.loc[is_cn, 'IGST_Amount'] = -df.loc[is_cn, 'IGST_Amount'].abs()
                df.loc[is_cn, 'CGST_Amount'] = -df.loc[is_cn, 'CGST_Amount'].abs()
                df.loc[is_cn, 'SGST_Amount'] = -df.loc[is_cn, 'SGST_Amount'].abs()
            else:
                df['Taxable_Value'] = -df['Taxable_Value'].abs()
                df['IGST_Amount'] = -df['IGST_Amount'].abs()
                df['CGST_Amount'] = -df['CGST_Amount'].abs()
                df['SGST_Amount'] = -df['SGST_Amount'].abs()
        
        df.loc[df['GSTR_Is_RCM'], ['IGST_Amount', 'CGST_Amount', 'SGST_Amount']] = 0.0
        
        core_cols = ['Invoice_No', 'Taxable_Value', 'Rate', 'IGST_Amount', 'CGST_Amount', 'SGST_Amount', 'GSTR_Is_RCM']
        if 'Invoice_Date' in df.columns: core_cols.append('Invoice_Date')
        if is_amendment and 'Original_Invoice_No' in df.columns: core_cols.append('Original_Invoice_No')
            
        existing_cols = [c for c in core_cols if c in df.columns]
        df = df[existing_cols].copy()
        
        df['Section'] = section_name
        df['Is_Amended'] = is_amendment
        
        df['Match_Key'] = clean_match_key(df['Invoice_No'])
        if is_amendment and 'Original_Invoice_No' in df.columns:
            df['Original_Match_Key'] = clean_match_key(df['Original_Invoice_No'])
        else:
            df['Original_Match_Key'] = None
            
        if section_name not in ['B2CS', 'B2CSA']:
            df = df[df['Match_Key'] != '']
            
        # Extract dates strictly for grouping (We NO LONGER drop GSTR-1 rows based on date)
        if 'Invoice_Date' in df.columns:
            df['Invoice_Date'] = pd.to_datetime(df['Invoice_Date'], errors='coerce', format='mixed', dayfirst=True)
            df['Month_Sort'] = df['Invoice_Date'].dt.strftime('%Y-%m')
            df['Month_Year'] = df['Invoice_Date'].dt.strftime('%b-%Y')
        else:
            df['Month_Sort'] = pd.NA
            df['Month_Year'] = pd.NA
            
        return df

    b2b = standardize_gstr(dfs.get('B2B', pd.DataFrame()), 'B2B', False)
    b2ba = standardize_gstr(dfs.get('B2BA', pd.DataFrame()), 'B2BA', True)
    exp = standardize_gstr(dfs.get('EXP', pd.DataFrame()), 'EXP', False)
    expa = standardize_gstr(dfs.get('EXPA', pd.DataFrame()), 'EXPA', True)
    b2cl = standardize_gstr(dfs.get('B2CL', pd.DataFrame()), 'B2CL', False)
    b2cla = standardize_gstr(dfs.get('B2CLA', pd.DataFrame()), 'B2CLA', True)
    b2cs = standardize_gstr(dfs.get('B2CS', pd.DataFrame()), 'B2CS', False)
    b2csa = standardize_gstr(dfs.get('B2CSA', pd.DataFrame()), 'B2CSA', True)
    cdnr = standardize_gstr(dfs.get('CDNR', pd.DataFrame()), 'CDNR', False)
    cdnra = standardize_gstr(dfs.get('CDNRA', pd.DataFrame()), 'CDNRA', True)

    reco_rows = []
    gross_total = 0.0
    cdnr_total = 0.0

    def apply_amendments_with_stats(original_df, amended_df, base_name, amended_name, is_cdnr=False):
        nonlocal gross_total, cdnr_total
        
        orig_val = original_df['Taxable_Value'].sum() if not original_df.empty else 0.0
        amend_val = amended_df['Taxable_Value'].sum() if not amended_df.empty else 0.0

        removed_val = 0.0
        purged_df = original_df.copy() if not original_df.empty else pd.DataFrame()
        amended_clean = pd.DataFrame()

        if not amended_df.empty:
            if not original_df.empty and 'Original_Match_Key' in amended_df.columns:
                hit_list = amended_df['Original_Match_Key'].dropna().tolist()
                removed_df = original_df[original_df['Match_Key'].isin(hit_list)]
                removed_val = removed_df['Taxable_Value'].sum()
                purged_df = original_df[~original_df['Match_Key'].isin(hit_list)].copy()
            
            amended_clean = amended_df.drop(columns=['Original_Match_Key'], errors='ignore')

        net_val = orig_val + amend_val - removed_val
        
        if is_cdnr: cdnr_total += net_val
        else: gross_total += net_val

        reco_rows.append({'Description': base_name, 'Amount': orig_val})
        reco_rows.append({'Description': amended_name, 'Amount': amend_val})
        reco_rows.append({'Description': f"Less: Original {base_name}", 'Amount': -removed_val})
        reco_rows.append({'Description': f"Net {base_name}", 'Amount': net_val})
        reco_rows.append({'Description': "", 'Amount': None}) 

        if purged_df.empty and amended_clean.empty: return pd.DataFrame()
        return pd.concat([purged_df, amended_clean], ignore_index=True)

    final_b2b = apply_amendments_with_stats(b2b, b2ba, 'B2B', 'B2BA')
    final_b2cl = apply_amendments_with_stats(b2cl, b2cla, 'B2CL', 'B2CLA')
    final_exp = apply_amendments_with_stats(exp, expa, 'EXP', 'EXPA')
    final_b2cs = apply_amendments_with_stats(b2cs, b2csa, 'B2CS', 'B2CSA')

    reco_rows.append({'Description': 'SUBTOTAL (GROSS TURNOVER)', 'Amount': gross_total})
    reco_rows.append({'Description': ' ', 'Amount': None}) 

    final_cdnr = apply_amendments_with_stats(cdnr, cdnra, 'CDNR', 'CDNRA', is_cdnr=True)

    total_net = gross_total + cdnr_total
    reco_rows.append({'Description': 'TOTAL NET GSTR-1 TURNOVER', 'Amount': total_net})

    reco_table = pd.DataFrame(reco_rows)

    master_gstr = pd.concat([final_b2b, final_exp, final_b2cl, final_cdnr], ignore_index=True)
    gstr_summary_data = pd.concat([master_gstr, final_b2cs], ignore_index=True)
    
    # Fill in blanks (like B2CS) with the user's selected UI start date
    def_sort = start_ts.strftime('%Y-%m')
    def_month = start_ts.strftime('%b-%Y')
    gstr_summary_data['Month_Sort'] = gstr_summary_data['Month_Sort'].fillna(def_sort)
    gstr_summary_data['Month_Year'] = gstr_summary_data['Month_Year'].fillna(def_month)
            
    return master_gstr, gstr_summary_data, reco_table


def process_zoho(df_raw, selected_branch="All Branches", is_credit_note=False, start_ts=None, end_ts=None):
    """Phase 4: Ingest Zoho, Multi-Currency Handling (Taxable Value only), STRICT Date Filters."""
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()
    df = df.loc[:, ~df.columns.duplicated()] 
    
    col_mapping = {}
    
    if is_credit_note:
        for col in df.columns:
            c_upper = str(col).upper()
            if 'CREDIT NOTE NUMBER' in c_upper or 'CREDIT NOTE NO' in c_upper:
                col_mapping['Invoice_No'] = col
            elif 'CREDIT NOTE DATE' in c_upper:
                col_mapping['Invoice_Date'] = col

    for col in df.columns:
        c_upper = str(col).upper()
        if 'Invoice_Date' not in col_mapping and any(x in c_upper for x in ['INVOICE DATE', 'DATE', 'CREDIT NOTE DATE']) and 'PARSED' not in c_upper and 'DUE' not in c_upper and 'ORIGINAL' not in c_upper:
            col_mapping['Invoice_Date'] = col
        elif 'Invoice_No' not in col_mapping and any(x in c_upper for x in ['INVOICE NUMBER', 'INVOICE NO', 'INV NO', 'CREDIT NOTE NUMBER', 'CREDIT NOTE NO']) and 'ORIGINAL' not in c_upper:
            col_mapping['Invoice_No'] = col
        elif 'Taxable_Value' not in col_mapping and any(x in c_upper for x in ['ITEM TOTAL', 'TAXABLE VALUE', 'SUBTOTAL', 'AMOUNT']):
            col_mapping['Taxable_Value'] = col
        elif 'CGST' not in col_mapping and 'CGST' in c_upper and 'RATE' not in c_upper: col_mapping['CGST'] = col
        elif 'SGST' not in col_mapping and 'SGST' in c_upper and 'RATE' not in c_upper: col_mapping['SGST'] = col
        elif 'IGST' not in col_mapping and 'IGST' in c_upper and 'RATE' not in c_upper: col_mapping['IGST'] = col
        elif 'Status' not in col_mapping and 'STATUS' in c_upper: col_mapping['Status'] = col
        elif 'Customer' not in col_mapping and 'CUSTOMER NAME' in c_upper: col_mapping['Customer'] = col
        elif 'Exchange_Rate' not in col_mapping and 'EXCHANGE RATE' in c_upper: col_mapping['Exchange_Rate'] = col
        elif 'Branch' not in col_mapping and 'BRANCH' in c_upper: col_mapping['Branch'] = col
        elif 'Zoho_RCM_Tax' not in col_mapping and 'REVERSE CHARGE TAX NAME' in c_upper: col_mapping['Zoho_RCM_Tax'] = col

    if 'Branch' in col_mapping and selected_branch != "All Branches":
        branch_col_name = col_mapping['Branch']
        df = df[df[branch_col_name].astype(str).str.strip() == selected_branch]

    # Note: Status filtering is duplicated safely here for non-ZIP uploads
    if 'Status' in col_mapping:
        status_col = col_mapping['Status']
        df = df[~df[status_col].astype(str).str.upper().isin(['DRAFT', 'VOID', 'CANCELLED'])]

    inv_col = col_mapping.get('Invoice_No')
    if not inv_col: return pd.DataFrame() 
    
    df = df.dropna(subset=[inv_col])
    
    df['Original_Zoho_Invoice_No'] = df[inv_col].astype(str).str.strip()
    df['Match_Key'] = clean_match_key(df[inv_col])
    
    df = df[df['Match_Key'] != '']
    
    if 'Zoho_RCM_Tax' in col_mapping:
        rcm_series = df[col_mapping['Zoho_RCM_Tax']].astype(str).str.strip().str.upper()
        df['Zoho_Is_RCM'] = (rcm_series != 'NAN') & (rcm_series != 'NONE') & (rcm_series != '')
    else:
        df['Zoho_Is_RCM'] = False

    if 'CGST' in col_mapping: df.loc[df['Zoho_Is_RCM'], col_mapping['CGST']] = 0.0
    if 'SGST' in col_mapping: df.loc[df['Zoho_Is_RCM'], col_mapping['SGST']] = 0.0
    if 'IGST' in col_mapping: df.loc[df['Zoho_Is_RCM'], col_mapping['IGST']] = 0.0

    if 'Invoice_Date' in col_mapping:
        df['Cleaned_Invoice_Date'] = pd.to_datetime(df[col_mapping['Invoice_Date']], errors='coerce', format='mixed', dayfirst=True)
        
        # 🚨 STRICT MANUAL ZOHO DATE SLICER 🚨
        if start_ts is not None and end_ts is not None:
            mask = df['Cleaned_Invoice_Date'].notna() & (df['Cleaned_Invoice_Date'] >= start_ts) & (df['Cleaned_Invoice_Date'] <= end_ts)
            df = df[mask]
        
        df['Month_Sort'] = df['Cleaned_Invoice_Date'].dt.strftime('%Y-%m').fillna(start_ts.strftime('%Y-%m'))
        df['Month_Year'] = df['Cleaned_Invoice_Date'].dt.strftime('%b-%Y').fillna(start_ts.strftime('%b-%Y'))
    else:
        df['Month_Sort'] = start_ts.strftime('%Y-%m')
        df['Month_Year'] = start_ts.strftime('%b-%Y')

    if 'Exchange_Rate' in col_mapping:
        df['Calculated_Exchange_Rate'] = pd.to_numeric(df[col_mapping['Exchange_Rate']], errors='coerce').fillna(1.0)
    else:
        df['Calculated_Exchange_Rate'] = 1.0
    
    if 'Taxable_Value' in col_mapping:
        base_tv = pd.to_numeric(df[col_mapping['Taxable_Value']], errors='coerce').fillna(0)
        calc_tv = base_tv * df['Calculated_Exchange_Rate']
        
        if is_credit_note: df[col_mapping['Taxable_Value']] = -abs(calc_tv)
        else: df[col_mapping['Taxable_Value']] = calc_tv
    else:
        df['Taxable_Value'] = 0.0

    tax_cols = ['CGST', 'SGST', 'IGST']
    for t in tax_cols:
        if t in col_mapping:
            base_tax = pd.to_numeric(df[col_mapping[t]], errors='coerce').fillna(0)
            if is_credit_note: df[col_mapping[t]] = -abs(base_tax)
            else: df[col_mapping[t]] = base_tax
        else:
            df[t] = 0.0

    agg_dict = {
        col_mapping.get('Taxable_Value', 'Taxable_Value'): 'sum',
        col_mapping.get('CGST', 'CGST'): 'sum',
        col_mapping.get('SGST', 'SGST'): 'sum',
        col_mapping.get('IGST', 'IGST'): 'sum',
        'Zoho_Is_RCM': 'max',
        'Month_Sort': 'first',
        'Month_Year': 'first',
        'Original_Zoho_Invoice_No': 'first'
    }
    
    if 'Customer' in col_mapping: agg_dict[col_mapping['Customer']] = 'first'
    if 'Branch' in col_mapping: agg_dict[col_mapping['Branch']] = 'first'
        
    zoho_agg = df.groupby('Match_Key').agg(agg_dict).reset_index()
    
    zoho_agg.rename(columns={
        col_mapping.get('Taxable_Value', 'Taxable_Value'): 'Zoho_Taxable_Value',
        col_mapping.get('CGST', 'CGST'): 'Zoho_CGST',
        col_mapping.get('SGST', 'SGST'): 'Zoho_SGST',
        col_mapping.get('IGST', 'IGST'): 'Zoho_IGST',
        col_mapping.get('Branch', 'Branch'): 'Zoho_Branch'
    }, inplace=True)
    
    return zoho_agg

def generate_monthly_summary(gstr_data, zoho_data):
    if gstr_data.empty: return pd.DataFrame()
    
    gstr_data['Total_Tax'] = gstr_data['IGST_Amount'] + gstr_data['CGST_Amount'] + gstr_data['SGST_Amount']
    g_sum = gstr_data.groupby(['Month_Sort', 'Month_Year']).agg(
        GSTR_Taxable_Value=('Taxable_Value', 'sum'),
        GSTR_Total_Tax=('Total_Tax', 'sum'),
        GSTR_CGST=('CGST_Amount', 'sum'),
        GSTR_SGST=('SGST_Amount', 'sum'),
        GSTR_IGST=('IGST_Amount', 'sum')
    ).reset_index()

    if not zoho_data.empty:
        zoho_data['Zoho_Total_Tax'] = zoho_data['Zoho_CGST'] + zoho_data['Zoho_SGST'] + zoho_data['Zoho_IGST']
        z_sum = zoho_data.groupby(['Month_Sort', 'Month_Year']).agg(
            Zoho_Taxable_Value=('Zoho_Taxable_Value', 'sum'),
            Zoho_Total_Tax=('Zoho_Total_Tax', 'sum'),
            Zoho_CGST=('Zoho_CGST', 'sum'),
            Zoho_SGST=('Zoho_SGST', 'sum'),
            Zoho_IGST=('Zoho_IGST', 'sum')
        ).reset_index()
    else:
        z_sum = pd.DataFrame(columns=['Month_Sort', 'Month_Year', 'Zoho_Taxable_Value', 'Zoho_Total_Tax', 'Zoho_CGST', 'Zoho_SGST', 'Zoho_IGST'])

    summary = pd.merge(g_sum, z_sum, on=['Month_Sort', 'Month_Year'], how='outer').fillna(0)
    summary = summary.sort_values('Month_Sort').reset_index(drop=True)
    summary = summary.drop(columns=['Month_Sort'])

    summary['Diff_Taxable'] = summary['GSTR_Taxable_Value'] - summary['Zoho_Taxable_Value']
    summary['Diff_Total_Tax'] = summary['GSTR_Total_Tax'] - summary['Zoho_Total_Tax']

    cols = [
        'Month_Year',
        'GSTR_Taxable_Value', 'Zoho_Taxable_Value', 'Diff_Taxable',
        'GSTR_Total_Tax', 'Zoho_Total_Tax', 'Diff_Total_Tax',
        'GSTR_CGST', 'Zoho_CGST',
        'GSTR_SGST', 'Zoho_SGST',
        'GSTR_IGST', 'Zoho_IGST'
    ]
    return summary[cols]


def reconcile(gstr_df, zoho_df):
    """Phase 5: The Grand Match Engine using Outer Join."""
    if gstr_df.empty: 
        gstr_df = pd.DataFrame(columns=['Match_Key', 'Invoice_No', 'Section', 'Is_Amended', 'Taxable_Value', 'CGST_Amount', 'SGST_Amount', 'IGST_Amount', 'GSTR_Is_RCM'])
    if zoho_df.empty: 
        zoho_df = pd.DataFrame(columns=['Match_Key', 'Zoho_Taxable_Value', 'Zoho_CGST', 'Zoho_SGST', 'Zoho_IGST', 'Zoho_Is_RCM', 'Original_Zoho_Invoice_No'])

    gstr_agg = gstr_df.groupby(['Match_Key', 'Invoice_No', 'Section', 'Is_Amended'], dropna=False).agg(
        GSTR_Taxable_Value=('Taxable_Value', 'sum'),
        GSTR_CGST=('CGST_Amount', 'sum'),
        GSTR_SGST=('SGST_Amount', 'sum'),
        GSTR_IGST=('IGST_Amount', 'sum'),
        GSTR_Is_RCM=('GSTR_Is_RCM', 'max')
    ).reset_index()
    
    reco = pd.merge(gstr_agg, zoho_df, on='Match_Key', how='outer', indicator=True)
    
    val_cols = ['GSTR_Taxable_Value', 'GSTR_CGST', 'GSTR_SGST', 'GSTR_IGST', 
                'Zoho_Taxable_Value', 'Zoho_CGST', 'Zoho_SGST', 'Zoho_IGST']
    for c in val_cols:
        if c not in reco.columns: reco[c] = 0.0
    reco[val_cols] = reco[val_cols].fillna(0)
    
    reco['Diff_Taxable'] = reco['GSTR_Taxable_Value'] - reco['Zoho_Taxable_Value']
    reco['GSTR_Total_Tax'] = reco['GSTR_CGST'] + reco['GSTR_SGST'] + reco['GSTR_IGST']
    reco['Zoho_Total_Tax'] = reco['Zoho_CGST'] + reco['Zoho_SGST'] + reco['Zoho_IGST']
    reco['Diff_Total_Tax'] = reco['GSTR_Total_Tax'] - reco['Zoho_Total_Tax']
    
    reco['GSTR_Is_RCM'] = reco['GSTR_Is_RCM'].fillna(False)
    reco['Zoho_Is_RCM'] = reco['Zoho_Is_RCM'].fillna(False)
    
    def assign_status(row):
        if row['_merge'] == 'left_only': return "Missing in Zoho"
        if row['_merge'] == 'right_only': return "Missing in GSTR-1"
        
        rcm_mismatch = bool(row['GSTR_Is_RCM']) != bool(row['Zoho_Is_RCM'])
        val_mismatch = abs(row['Diff_Taxable']) > 1.0 or abs(row['Diff_Total_Tax']) > 1.0
        
        if rcm_mismatch and val_mismatch: return "Value & RCM Mismatch"
        if rcm_mismatch: return "RCM Status Mismatch"
        if val_mismatch: return "Value Mismatch"
        
        return "Matched (Amended)" if row.get('Is_Amended', False) else "Perfect Match"

    reco['Match_Status'] = reco.apply(assign_status, axis=1)
    
    if 'Original_Zoho_Invoice_No' in reco.columns:
        reco['Original_Zoho_Invoice_No'] = reco['Original_Zoho_Invoice_No'].replace(['nan', 'None', 'NaN', ''], pd.NA)
        reco['Invoice_No'] = reco['Invoice_No'].fillna(reco['Original_Zoho_Invoice_No'].astype(str) + " (From Zoho)")
        reco['Invoice_No'] = reco['Invoice_No'].str.replace(r'<NA> \(From Zoho\)', 'Missing Invoice No', regex=True)
    else:
        reco['Invoice_No'] = reco['Invoice_No'].fillna(reco['Match_Key'] + " (From Zoho)")
        
    reco['GSTR_Is_RCM'] = reco['GSTR_Is_RCM'].map({True: 'Yes', False: 'No'})
    reco['Zoho_Is_RCM'] = reco['Zoho_Is_RCM'].map({True: 'Yes', False: 'No'})
    
    final_cols = [
        'Match_Status', 'Section', 'Invoice_No', 'Is_Amended',
        'GSTR_Is_RCM', 'Zoho_Is_RCM',
        'GSTR_Taxable_Value', 'Zoho_Taxable_Value', 'Diff_Taxable',
        'GSTR_Total_Tax', 'Zoho_Total_Tax', 'Diff_Total_Tax',
        'GSTR_CGST', 'GSTR_SGST', 'GSTR_IGST', 
        'Zoho_CGST', 'Zoho_SGST', 'Zoho_IGST'
    ]
    
    zoho_extras = [c for c in zoho_df.columns if c not in ['Match_Key', 'Month_Sort', 'Month_Year', 'Zoho_Taxable_Value', 'Zoho_CGST', 'Zoho_SGST', 'Zoho_IGST', 'Zoho_Is_RCM', 'Original_Zoho_Invoice_No']]
    for i, col in enumerate(zoho_extras):
        final_cols.insert(4 + i, col)

    final_cols = [c for c in final_cols if c in reco.columns]
    reco = reco[final_cols].sort_values(by='Match_Status', ascending=False)
    reco = reco.drop(columns=['_merge', 'Original_Zoho_Invoice_No'], errors='ignore')
    return reco

# ==========================================
# STREAMLIT UI (Frontend)
# ==========================================

st.title("📊 GST & Zoho Sales Reconciliation App")
st.markdown("Automate the reconciliation between your GSTR-1 Excel File, Zoho Sales Register, and Zoho Credit Notes.")

# --- FILE UPLOADS ---
col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("1. Upload GSTR-1")
    gstr_file = st.file_uploader("Upload Offline Tool Excel (GSTR-1)", type=['xlsx'])

with col2:
    st.subheader("2. Upload Zoho Sales")
    zoho_file = st.file_uploader("Upload Zoho Sales Register", type=['csv', 'xlsx', 'zip'])

with col3:
    st.subheader("3. Upload Zoho Credit Notes")
    zoho_cn_file = st.file_uploader("Upload Zoho Credit Notes (Optional)", type=['csv', 'xlsx', 'zip'])

st.divider()

# --- STRICT DATE FILTERS & BRANCH ---
st.subheader("⚙️ 4. Set Filters Before Running")

date_col1, date_col2, branch_col = st.columns(3)

today = datetime.date.today()
first_day = today.replace(day=1)

with date_col1:
    start_date = st.date_input("From Date (Slices Zoho Data)", first_day, format="DD/MM/YYYY")
with date_col2:
    end_date = st.date_input("To Date (Slices Zoho Data)", today, format="DD/MM/YYYY")

start_ts = pd.to_datetime(start_date)
end_ts = pd.to_datetime(end_date)

selected_branch = "All Branches"
zoho_raw_df = None
zoho_cn_raw_df = None
branches = set()

# Load files using the new ZIP-compatible function
if zoho_file:
    zoho_file.seek(0)
    zoho_raw_df = load_zoho_upload(zoho_file)
    if zoho_raw_df is not None:
        for col in zoho_raw_df.columns:
            if 'BRANCH' in str(col).upper():
                branches.update(zoho_raw_df[col].dropna().astype(str).str.strip().unique())
                break

if zoho_cn_file:
    zoho_cn_file.seek(0)
    zoho_cn_raw_df = load_zoho_upload(zoho_cn_file)
    if zoho_cn_raw_df is not None:
        for col in zoho_cn_raw_df.columns:
            if 'BRANCH' in str(col).upper():
                branches.update(zoho_cn_raw_df[col].dropna().astype(str).str.strip().unique())
                break

with branch_col:
    if branches:
        unique_branches = sorted(list(branches))
        if "" in unique_branches: unique_branches.remove("")
        unique_branches.insert(0, "All Branches")
        selected_branch = st.selectbox("Select Zoho Branch:", unique_branches)
    elif (zoho_file is not None) or (zoho_cn_file is not None):
        st.info("No 'Branch' column found.")


if gstr_file and (zoho_raw_df is not None or zoho_cn_raw_df is not None):
    st.write("") 
    if st.button("Run Reconciliation", type="primary", use_container_width=True):
        with st.spinner(f"Slicing Zoho from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}..."):
            try:
                # 1. Process GSTR-1 (Untouched by Dates!)
                master_gstr, gstr_summary_data, gstr_reco_table = process_gstr1(gstr_file, start_ts)
                
                # 2. Process Zoho (Strictly Sliced!)
                zoho_agg = pd.DataFrame()
                if zoho_raw_df is not None:
                    zoho_sr_agg = process_zoho(zoho_raw_df, selected_branch, False, start_ts, end_ts)
                    zoho_agg = pd.concat([zoho_agg, zoho_sr_agg], ignore_index=True)
                    
                if zoho_cn_raw_df is not None:
                    zoho_cn_agg = process_zoho(zoho_cn_raw_df, selected_branch, True, start_ts, end_ts)
                    zoho_agg = pd.concat([zoho_agg, zoho_cn_agg], ignore_index=True)
                
                # 3. Match & Combine
                reco_report = reconcile(master_gstr, zoho_agg)
                monthly_summary = generate_monthly_summary(gstr_summary_data, zoho_agg)
                
                matched_invoices_df = reco_report[~reco_report['Match_Status'].isin(['Missing in Zoho', 'Missing in GSTR-1'])].copy()
                missing_in_zoho_df = reco_report[reco_report['Match_Status'] == 'Missing in Zoho'].copy()
                missing_in_gstr_df = reco_report[reco_report['Match_Status'] == 'Missing in GSTR-1'].copy()
                
                st.success(f"Reconciliation Complete! Zoho data sliced to exactly {start_date.strftime('%d-%b-%Y')} - {end_date.strftime('%d-%b-%Y')}")
                
                # --- UI DISPLAY ---
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "GSTR-1 Reco Ledger", "Matched Invoices", "Missing in Zoho", "Missing in GSTR-1", "Side-by-Side Monthly Summary"
                ])
                
                with tab1:
                    st.markdown("### GSTR-1 Mathematical Breakdown")
                    
                    def highlight_rows(row):
                        desc = str(row['Description']).strip()
                        if desc.startswith('Net ') or 'SUBTOTAL' in desc:
                            return ['background-color: #f0f2f6; font-weight: bold'] * len(row)
                        if 'TOTAL' in desc:
                            return ['background-color: #d4edda; font-weight: bold; color: black'] * len(row)
                        if 'CDNR' in desc and not desc.startswith('Net'):
                            return ['color: #d9534f'] * len(row)
                        return [''] * len(row)
                        
                    st.dataframe(gstr_reco_table.style.apply(highlight_rows, axis=1).format({
                        'Amount': "{:,.2f}"
                    }, na_rep=""), use_container_width=True, height=600)
                    
                with tab2:
                    st.dataframe(matched_invoices_df, use_container_width=True)
                with tab3:
                    st.dataframe(missing_in_zoho_df, use_container_width=True)
                with tab4:
                    st.dataframe(missing_in_gstr_df, use_container_width=True)
                with tab5:
                    st.dataframe(monthly_summary, use_container_width=True)
                
                # --- EXCEL EXPORT ---
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    gstr_reco_table.to_excel(writer, sheet_name='GSTR1_Calculation', index=False)
                    monthly_summary.to_excel(writer, sheet_name='Monthly_Net_Summary', index=False)
                    matched_invoices_df.to_excel(writer, sheet_name='Matched_Invoices', index=False)
                    missing_in_zoho_df.to_excel(writer, sheet_name='Missing_in_Zoho', index=False)
                    missing_in_gstr_df.to_excel(writer, sheet_name='Missing_in_GSTR1', index=False)
                output.seek(0)
                
                st.markdown("### 📥 Download Reports")
                file_suffix = "All_Branches" if selected_branch == "All Branches" else selected_branch.replace(" ", "_")
                st.download_button(
                    label="Download Multi-Sheet Excel Report",
                    data=output,
                    file_name=f"Reconciliation_Report_{file_suffix}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
            except Exception as e:
                st.error("❌ An error occurred! Please check the details below:")
                st.code(traceback.format_exc())