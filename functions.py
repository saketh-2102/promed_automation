import pandas as pd

def load_and_prepare_data(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    df.columns = df.columns.str.strip().str.upper()
    
    required_cols = ["NET AMOUNT", "IP NUMBER", "ADMITING DEPARTMENT", "HEADER", "SERVICE NAME"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not found. Please check the actual column names in the file.")
    
    df["NET AMOUNT"] = df["NET AMOUNT"].apply(clean_net_amount)
    df["CATEGORY"] = df["IP NUMBER"].apply(lambda x: "IP Revenue" if str(x).startswith("IP") else "OP Revenue")
    
    return df

def clean_net_amount(amount):
    if pd.isna(amount):
        return 0
    amount = str(amount).replace(",", "").strip()
    if "(" in amount and ")" in amount:
        return -float(amount.strip("()"))
    return float(amount)

def map_department(dept):
    department_mapping = {
        "Cardiology": ["INTERVENTIONAL CARDIOLOGIST", "CARDIOLOGY", "CARDIOTHORACIC SURGEON"],
        "General Medicine": ["ACCIDENT  CRITICAL AND EMERGENCY CARE PHYSICIAN", "INTERNAL MEDICINE", "GENERAL MEDICINE"],
        "Orthopedics": ["ORTHOPAEDICS"],
        "General Surgery": ["GENERAL SURGEON"],
        "Paediatrics": ["PAEDIATRICS"],
        "Gynecology": ["GYNECOLOGY", "OBSTETRICS & GYNAECOLOGY"],
        "Urology": ["UROLOGY"],
        "Others": [""]
    }
    dept = str(dept).strip().upper()
    for key, values in department_mapping.items():
        if any(value in dept for value in values):
            return key
    return "Others"

def process_revenue_data(df):
    # Process IP Revenue
    df_ip = df[df["CATEGORY"] == "IP Revenue"].copy()
    df_ip["ADMITTING CATEGORY"] = df_ip["ADMITING DEPARTMENT"].apply(map_department)
    
    ip_summary = df_ip.groupby("ADMITTING CATEGORY").agg(
        UNIQUE_PATIENTS=("IP NUMBER", "nunique"),
        TOTAL_AMOUNT=("NET AMOUNT", "sum")
    ).reset_index()
    
    total_ip_row = pd.DataFrame([["Total", ip_summary["UNIQUE_PATIENTS"].sum(), ip_summary["TOTAL_AMOUNT"].sum()]], 
                               columns=["ADMITTING CATEGORY", "UNIQUE_PATIENTS", "TOTAL_AMOUNT"])
    final_ip_result = pd.concat([ip_summary, total_ip_row], ignore_index=True)
    
    # Process OP Revenue
    df_op = df[df["CATEGORY"] == "OP Revenue"].copy()
    
    # Define headers for different categories
    consultation_headers = ["CONSULTATION CHARGES", "CONSULTATION CHARGES"]
    procedure_headers = ["PROCEDURE"]
    hospital_charges_headers = ["HOSPITAL CHARGES", "HOSPITAL CHARGES"]
    mhc_package_headers = ["MHC PACKAGE", "MHC PACKAGE"]
    
    # Create masks for different categories
    others_revenue_mask = (
        (df_op["HEADER"].str.strip().str.upper() == "PHYSIOTHERAPY") |
        ((df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in consultation_headers])) &
        (df_op["SERVICE NAME"].str.strip().str.upper().str.contains("PRIYA DHARSHINI D", case=False)) |
        (df_op["HEADER"].str.strip().str.upper() == "AMBULANCE SERVICE") |
        (df_op["HEADER"].str.strip().str.upper() == "EQUIPMENT")
    ))
    
    nursing_visit_mask = (
        ((df_op["HEADER"].str.strip().str.upper() == "INVESTIGATION VISIT") & 
         (df_op["SERVICE NAME"].str.strip().str.upper() == "NURSE HOME VISIT")) |
        (df_op["HEADER"].str.strip().str.upper() == "NURSING HOME VISIT CHARGE") |
        ((df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in hospital_charges_headers])) &
        (df_op["SERVICE NAME"].str.strip().str.upper() == "SPECIAL NURSE CARE")
    ))
    
    health_checkup_mask = (
        (df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in mhc_package_headers])) 
    )
    
    cardiology_proc_mask = (
        (df_op["HEADER"].str.strip().str.upper() == "CARDIOLOGY") 
    )
    
    radiology_proc_mask = (
        (df_op["HEADER"].str.strip().str.upper() == "RADIOLOGY") 
    )
    
    other_proc_charges_mask = (
        ((df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in hospital_charges_headers])) &
         (df_op["SERVICE NAME"].str.strip().str.upper().isin([
             "IV FLUID THERAPY PER HOUR", "REGISTRATION FEE"
         ]))) |
        ((df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in procedure_headers])) &
         (df_op["SERVICE NAME"].str.strip().str.upper().str.contains(
             "SUTURE REMOVAL|SUTURING - MINOR|DRESSING|DIALYSIS", case=False))
        ) &
        ~nursing_visit_mask & ~others_revenue_mask
    )
    
    consultation_mask = (
        df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in consultation_headers]) &
        df_op["ADMITING DEPARTMENT"].notna()
    )
    
    procedure_mask = (
        df_op["HEADER"].str.strip().str.upper().isin([h.upper() for h in procedure_headers]) &
        df_op["ADMITING DEPARTMENT"].notna() &
        (
            df_op["SERVICE NAME"].str.upper().str.contains("ORTHO PROCEDURE") |
            df_op["SERVICE NAME"].str.upper().str.contains("OP - PROCEDURE")
        ) 
    )
    
    
    
    op_consult_proc_mask = consultation_mask | procedure_mask
    consultation_df = df_op[consultation_mask]
    consultation_visits = consultation_df["IP NUMBER"].count()
    consultation_amount = consultation_df["NET AMOUNT"].sum()
    
    procedure_df = df_op[procedure_mask]
    procedure_visits = procedure_df["IP NUMBER"].count()
    procedure_amount = procedure_df["NET AMOUNT"].sum()
    
    print("\nDetailed Breakdown:")
    print(f"Consultation - Visits: {consultation_visits}, Amount: {consultation_amount:.2f}")
    print(f"Procedures - Visits: {procedure_visits}, Amount: {procedure_amount:.2f}")
    print(f"Combined Total: {consultation_amount + procedure_amount:.2f}\n")
    
    
    # New rule for Laboratory Revenue
    lab_revenue_mask = (
        (df_op["HEADER"].str.strip().str.upper().isin(["LABORATORY", "PACKAGE", "HAEMATOLOGY"])) |
        ((df_op["HEADER"].str.strip().str.upper() == "INVESTIGATION VISIT"))
    )
    
    df_op["OP_CATEGORY"] = "Other OP Revenue"
    df_op.loc[consultation_mask | procedure_mask, "OP_CATEGORY"] = "OP Consultation & Procedures"
    df_op.loc[cardiology_proc_mask, "OP_CATEGORY"] = "OP Cardiology Procedures"
    df_op.loc[radiology_proc_mask, "OP_CATEGORY"] = "OP Radiology"
    df_op.loc[other_proc_charges_mask, "OP_CATEGORY"] = "Other Procedure & Charges"
    df_op.loc[health_checkup_mask, "OP_CATEGORY"] = "OP Health Checkup Packages"
    df_op.loc[nursing_visit_mask, "OP_CATEGORY"] = "OP Nursing Home Visit"
    df_op.loc[others_revenue_mask, "OP_CATEGORY"] = "OP Others Revenue"
    df_op.loc[lab_revenue_mask, "OP_CATEGORY"] = "OP Laboratory Revenue"
    
    df_op_summary = df_op.groupby("OP_CATEGORY").agg(
        TOTAL_VISITS=("IP NUMBER", "count"),
        TOTAL_AMOUNT=("NET AMOUNT", "sum")
    ).reset_index()
    df_op_summary = df_op_summary.rename(columns={"OP_CATEGORY": "ADMITTING CATEGORY"})
    
    category_order = [
        "OP Consultation & Procedures",
        "OP Cardiology Procedures",
        "OP Radiology",
        "OP Health Checkup Packages",
        "OP Nursing Home Visit",
        "OP Others Revenue",
        "OP Laboratory Revenue",
        "Other Procedure & Charges",
    ]
    df_op_summary = df_op_summary.set_index("ADMITTING CATEGORY").loc[category_order].reset_index()
    df_op_summary.loc[
    df_op_summary["ADMITTING CATEGORY"] == "OP Consultation & Procedures",
    "TOTAL_AMOUNT"
] = consultation_amount + procedure_amount

    
    # Create a single Excel file with multiple sheets
    with pd.ExcelWriter("revenue_summary.xlsx") as writer:
        final_ip_result.to_excel(writer, sheet_name="IP Revenue", index=False)
        df_op_summary.to_excel(writer, sheet_name="OP Revenue", index=False)
    
    print("Revenue reports saved to revenue_summary.xlsx with separate sheets for IP and OP revenue.")