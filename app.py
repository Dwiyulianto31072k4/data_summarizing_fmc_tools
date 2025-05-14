# =========================================================
# Fix broken "|"‑delimited TXT  ➜  CSV bersih (Streamlit ver.)
# =========================================================
import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import csv
import os
import time
import tempfile
import concurrent.futures
from tqdm.auto import tqdm
import io
import base64

# Konfigurasi halaman Streamlit
st.set_page_config(
    page_title="Fix TXT to CSV Converter",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Judul aplikasi
st.title("🔄 Konverter TXT ke CSV untuk File Berat")
st.markdown("Memperbaiki file TXT dengan delimiter yang rusak dan mengkonversinya ke CSV")

# Fungsi utama untuk memperbaiki file
def fix_file(uploaded_file, n_cols, delim, encoding_in, encoding_out, chunk_size=10000, max_buffer_size=100):
    """
    Memperbaiki file TXT yang rusak dan mengkonversinya ke CSV
    """
    start_time = time.time()
    
    # Buat tempfile untuk menyimpan hasil
    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_reject = tempfile.NamedTemporaryFile(delete=False, suffix='.reject.txt')
    
    # Get file size in MB
    file_size_mb = uploaded_file.size / (1024 * 1024)
    
    # Baca file yang diupload
    content = uploaded_file.getvalue().decode(encoding_in, errors='replace')
    lines = content.splitlines()
    
    ok = bad = 0
    buffer = []
    ok_rows = []  # Buffer untuk menyimpan baris yang valid
    
    # Setup progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    stats_text = st.empty()
    
    try:
        with open(temp_csv.name, 'w', encoding=encoding_out, newline='') as fh_ok, \
             open(temp_reject.name, 'w', encoding=encoding_out) as fh_bad:
            
            writer_ok = csv.writer(fh_ok)
            
            for i, raw in enumerate(lines):
                # Update progress
                progress = min(i / max(len(lines), 1), 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Memproses baris {i+1} dari {len(lines)}")
                
                # Update statistik setiap 1000 baris
                if i % 1000 == 0:
                    elapsed = time.time() - start_time
                    speed = (i / len(lines) * file_size_mb) / max(elapsed, 0.001)
                    stats_text.text(f"OK: {ok:,} | Reject: {bad:,} | Kecepatan: {speed:.2f} MB/s")
                
                # Proses baris
                parts = raw.rstrip("\n").split(delim)
                
                if len(parts) == n_cols and not buffer:
                    # Baris valid, simpan ke buffer sementara
                    ok_rows.append(parts)
                    ok += 1
                    
                    # Tulis dalam chunk jika buffer penuh
                    if len(ok_rows) >= chunk_size:
                        writer_ok.writerows(ok_rows)
                        ok_rows = []
                    
                    continue
                
                buffer.extend(parts)
                
                # Batas keamanan (jika buffer terlalu besar)
                if len(buffer) > n_cols * max_buffer_size:
                    fh_bad.write(delim.join(buffer) + "\n")
                    bad += 1
                    buffer = []
                    continue
                
                if len(buffer) == n_cols:
                    ok_rows.append(buffer)
                    ok += 1
                    buffer = []
                    
                    # Tulis dalam chunk jika buffer penuh
                    if len(ok_rows) >= chunk_size:
                        writer_ok.writerows(ok_rows)
                        ok_rows = []
                        
                elif len(buffer) > n_cols:
                    fh_bad.write(delim.join(buffer) + "\n")
                    bad += 1
                    buffer = []
            
            # Tulis sisa baris valid
            if ok_rows:
                writer_ok.writerows(ok_rows)
                
            # Periksa sisa buffer di EOF
            if buffer:
                fh_bad.write(delim.join(buffer) + "\n")
                bad += 1
        
        # Complete progress bar
        progress_bar.progress(1.0)
        duration = time.time() - start_time
        processing_speed = file_size_mb / max(duration, 0.001)
        
        status_text.text(f"Selesai! Memproses {len(lines):,} baris selesai dalam {duration:.2f} detik")
        stats_text.text(f"OK: {ok:,} | Reject: {bad:,} | Kecepatan: {processing_speed:.2f} MB/s")
        
        # Baca kedua file untuk diunduh
        with open(temp_csv.name, 'rb') as f_csv, open(temp_reject.name, 'rb') as f_reject:
            csv_data = f_csv.read()
            reject_data = f_reject.read()
        
        # Hapus file sementara
        os.unlink(temp_csv.name)
        os.unlink(temp_reject.name)
        
        return ok, bad, file_size_mb, duration, csv_data, reject_data
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
        # Hapus file sementara jika error
        try:
            os.unlink(temp_csv.name)
            os.unlink(temp_reject.name)
        except:
            pass
        return 0, 0, file_size_mb, 0, None, None

# Fungsi untuk membuat link download
def get_download_link(file_data, filename, text):
    """Membuat link download untuk file binary"""
    b64 = base64.b64encode(file_data).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

# Sidebar untuk konfigurasi
with st.sidebar:
    st.header("Konfigurasi")
    
    n_cols = st.number_input("Jumlah kolom ideal", min_value=1, value=30)
    delim = st.text_input("Delimiter", value="|")
    
    st.subheader("Encoding")
    encoding_in = st.selectbox(
        "Encoding Input",
        options=["utf-8", "latin1", "cp1252", "iso-8859-1"],
        index=0
    )
    
    encoding_out = st.selectbox(
        "Encoding Output",
        options=["utf-8", "latin1", "cp1252", "iso-8859-1"],
        index=0
    )
    
    st.subheader("Performa")
    chunk_size = st.number_input(
        "Chunk Size (baris)",
        min_value=100,
        max_value=50000,
        value=10000,
        help="Jumlah baris yang disimpan di memori sebelum ditulis ke file"
    )
    
    max_buffer_size = st.number_input(
        "Max Buffer Size Multiplier",
        min_value=1,
        max_value=1000,
        value=100,
        help="Menentukan ukuran maksimum buffer jika file rusak parah"
    )
    
    st.subheader("Petunjuk Penggunaan")
    st.markdown("""
    1. Unggah file TXT dengan delimiter rusak
    2. Sesuaikan jumlah kolom ideal
    3. Klik tombol "Fix dan Konversi"
    4. Unduh hasil konversi
    """)
    
    st.warning("⚠️ File besar bisa memakan waktu. Biarkan proses selesai.")

# Area utama
col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("Upload File TXT")
    uploaded_file = st.file_uploader("Pilih file TXT untuk dikonversi", type=["txt", "TXT"])
    
    if uploaded_file is not None:
        # Tampilkan info file
        file_details = {
            "Nama File": uploaded_file.name,
            "Tipe File": uploaded_file.type,
            "Ukuran": f"{uploaded_file.size / (1024*1024):.2f} MB"
        }
        
        st.write("Detail File:")
        for k, v in file_details.items():
            st.write(f"- **{k}:** {v}")
        
        # Preview file
        st.subheader("Preview (5 baris pertama):")
        content_preview = uploaded_file.getvalue().decode(encoding_in, errors='replace')
        lines = content_preview.splitlines()[:5]
        
        for i, line in enumerate(lines):
            st.text(f"{i+1}: {line[:100]}..." if len(line) > 100 else f"{i+1}: {line}")
        
        # Tombol konversi
        if st.button("Fix dan Konversi", type="primary"):
            with st.spinner("Memperbaiki dan mengkonversi file..."):
                ok, bad, file_size_mb, duration, csv_data, reject_data = fix_file(
                    uploaded_file, n_cols, delim, encoding_in, encoding_out, chunk_size, max_buffer_size
                )
                
                if csv_data and reject_data is not None:
                    st.success("Konversi selesai!")
                    
                    # Area unduhan
                    st.subheader("Unduh Hasil")
                    
                    col_dl1, col_dl2 = st.columns(2)
                    
                    with col_dl1:
                        csv_filename = f"{os.path.splitext(uploaded_file.name)[0]}.csv"
                        st.markdown(get_download_link(csv_data, csv_filename, f"⬇️ Unduh CSV ({ok:,} baris)"), unsafe_allow_html=True)
                    
                    with col_dl2:
                        reject_filename = f"{os.path.splitext(uploaded_file.name)[0]}.reject.txt"
                        st.markdown(get_download_link(reject_data, reject_filename, f"⬇️ Unduh Reject ({bad:,} baris)"), unsafe_allow_html=True)
                    
                    # Hasil statistik
                    st.subheader("Statistik Hasil")
                    stats_cols = st.columns(3)
                    
                    with stats_cols[0]:
                        st.metric("Baris Valid", f"{ok:,}")
                    
                    with stats_cols[1]:
                        st.metric("Baris Reject", f"{bad:,}")
                    
                    with stats_cols[2]:
                        processing_speed = file_size_mb / max(duration, 0.001)
                        st.metric("Kecepatan", f"{processing_speed:.2f} MB/s")
                    
                    # Jika ada data valid, tampilkan preview
                    if ok > 0 and csv_data:
                        st.subheader("Preview CSV Output (10 baris pertama)")
                        try:
                            # Gunakan StringIO untuk menghindari membaca seluruh file
                            csv_preview = io.StringIO(csv_data.decode(encoding_out))
                            df_preview = pd.read_csv(csv_preview, nrows=10)
                            st.dataframe(df_preview)
                        except Exception as e:
                            st.warning(f"Tidak dapat menampilkan preview CSV: {str(e)}")

with col2:
    st.subheader("Informasi")
    st.info("""
    **Tentang Aplikasi**
    
    Aplikasi ini memperbaiki file TXT dengan delimiter yang rusak dan mengkonversinya ke format CSV yang bersih.
    
    **Fitur:**
    - Memperbaiki baris dengan jumlah kolom yang salah
    - Menggabungkan baris yang terpencar
    - Mendukung file berukuran besar
    - Pemrosesan yang efisien dengan chunking
    - Penanganan karakter yang tidak valid
    
    **Kasus Penggunaan:**
    - File TXT ekspor dari database yang rusak
    - File log dengan format yang tidak konsisten
    - Data transaksi yang perlu dibersihkan
    
    Aplikasi ini menggunakan pemrosesan per baris untuk mengoptimalkan penggunaan memori.
    """)
    
    # Metrik penggunaan memori saat ini
    if uploaded_file is not None:
        ram_usage = os.popen('ps -p %d -o %cpu,%mem,rss,vsz | tail -n 1' % os.getpid()).read().strip()
        st.subheader("Penggunaan Sumber Daya Saat Ini")
        st.code(f"PID: {os.getpid()}\n{ram_usage}")

# Footer
st.markdown("---")
st.caption("Dibuat dengan ❤️ menggunakan Streamlit dan Python")
