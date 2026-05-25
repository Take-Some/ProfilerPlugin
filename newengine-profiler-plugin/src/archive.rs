use std::io::Write;
use std::path::Path;

use crate::util::zip_dos_datetime_from_unix_ms;

pub(crate) struct ZipFileEntry<'a> {
    pub(crate) name: String,
    pub(crate) bytes: &'a [u8],
}

struct CentralDirectoryRecord {
    name: Vec<u8>,
    crc32: u32,
    size: u32,
    local_header_offset: u32,
}

pub(crate) fn write_stored_zip(path: &Path, created_unix_ms: u128, entries: &[ZipFileEntry<'_>]) -> Result<(), String> {
    if entries.is_empty() {
        return Err("zip archive requires at least one entry".to_owned());
    }
    if entries.len() > u16::MAX as usize {
        return Err(format!("zip archive has too many entries: {}", entries.len()));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create archive parent '{}' failed: {e}", parent.display()))?;
    }

    let mut file = std::fs::File::create(path)
        .map_err(|e| format!("create archive '{}' failed: {e}", path.display()))?;
    let (dos_time, dos_date) = zip_dos_datetime_from_unix_ms(created_unix_ms);
    let mut central_records = Vec::with_capacity(entries.len());
    let mut offset = 0u64;

    for entry in entries {
        let name = normalize_zip_entry_name(&entry.name)?;
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(format!("zip entry name is too long: {name}"));
        }
        let size = checked_u32(entry.bytes.len(), "zip entry payload")?;
        let local_header_offset = checked_u32_from_u64(offset, "zip local header offset")?;
        let crc32 = crc32_ieee(entry.bytes);

        write_u32(&mut file, 0x0403_4b50)?;
        write_u16(&mut file, 20)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, dos_time)?;
        write_u16(&mut file, dos_date)?;
        write_u32(&mut file, crc32)?;
        write_u32(&mut file, size)?;
        write_u32(&mut file, size)?;
        write_u16(&mut file, name_bytes.len() as u16)?;
        write_u16(&mut file, 0)?;
        file.write_all(name_bytes)
            .map_err(|e| format!("write archive entry name '{name}' failed: {e}"))?;
        file.write_all(entry.bytes)
            .map_err(|e| format!("write archive entry '{name}' failed: {e}"))?;

        offset = checked_add_u64(offset, 30, "zip local header")?;
        offset = checked_add_u64(offset, name_bytes.len() as u64, "zip entry name")?;
        offset = checked_add_u64(offset, entry.bytes.len() as u64, "zip entry payload")?;

        central_records.push(CentralDirectoryRecord {
            name: name_bytes.to_vec(),
            crc32,
            size,
            local_header_offset,
        });
    }

    let central_dir_offset = checked_u32_from_u64(offset, "zip central directory offset")?;
    for record in &central_records {
        write_u32(&mut file, 0x0201_4b50)?;
        write_u16(&mut file, 20)?;
        write_u16(&mut file, 20)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, dos_time)?;
        write_u16(&mut file, dos_date)?;
        write_u32(&mut file, record.crc32)?;
        write_u32(&mut file, record.size)?;
        write_u32(&mut file, record.size)?;
        write_u16(&mut file, record.name.len() as u16)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, 0)?;
        write_u16(&mut file, 0)?;
        write_u32(&mut file, 0)?;
        write_u32(&mut file, record.local_header_offset)?;
        file.write_all(&record.name)
            .map_err(|e| format!("write central directory record failed: {e}"))?;

        offset = checked_add_u64(offset, 46, "zip central directory header")?;
        offset = checked_add_u64(offset, record.name.len() as u64, "zip central directory name")?;
    }

    let central_dir_size = checked_u32_from_u64(
        offset - central_dir_offset as u64,
        "zip central directory size",
    )?;
    let entry_count = central_records.len() as u16;

    write_u32(&mut file, 0x0605_4b50)?;
    write_u16(&mut file, 0)?;
    write_u16(&mut file, 0)?;
    write_u16(&mut file, entry_count)?;
    write_u16(&mut file, entry_count)?;
    write_u32(&mut file, central_dir_size)?;
    write_u32(&mut file, central_dir_offset)?;
    write_u16(&mut file, 0)?;
    file.flush().map_err(|e| format!("flush archive '{}' failed: {e}", path.display()))
}

fn normalize_zip_entry_name(name: &str) -> Result<String, String> {
    let normalized = name.trim().replace('\\', "/");
    if normalized.is_empty() {
        return Err("zip entry name cannot be empty".to_owned());
    }
    if normalized.starts_with('/') || normalized.contains(':') || normalized.split('/').any(|part| part.is_empty() || part == "." || part == "..") {
        return Err(format!("unsafe zip entry name: {name}"));
    }
    Ok(normalized)
}

fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn write_u16(file: &mut std::fs::File, value: u16) -> Result<(), String> {
    file.write_all(&value.to_le_bytes()).map_err(|e| e.to_string())
}

fn write_u32(file: &mut std::fs::File, value: u32) -> Result<(), String> {
    file.write_all(&value.to_le_bytes()).map_err(|e| e.to_string())
}

fn checked_u32(value: usize, what: &str) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{what} exceeds ZIP32 limit"))
}

fn checked_u32_from_u64(value: u64, what: &str) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{what} exceeds ZIP32 limit"))
}

fn checked_add_u64(a: u64, b: u64, what: &str) -> Result<u64, String> {
    a.checked_add(b).ok_or_else(|| format!("{what} overflow while writing ZIP archive"))
}
