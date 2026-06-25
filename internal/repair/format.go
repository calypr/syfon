package repair

import (
	"encoding/json"
	"fmt"
	"io"
)

func WriteAudit(w io.Writer, format string, report Report) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	if _, err := fmt.Fprintf(w, "scanned=%d findings=%d\n", report.Scanned, len(report.Objects)); err != nil {
		return err
	}
	for _, obj := range report.Objects {
		if _, err := fmt.Fprintf(w, "%s sha256=%s scope=%s/%s autofix=%t\n", obj.ObjectID, obj.SHA256, obj.Organization, obj.Project, obj.AutoFixable); err != nil {
			return err
		}
		for _, finding := range obj.Findings {
			if _, err := fmt.Fprintf(w, "  - %s [%s] %s\n", finding.Kind, finding.Severity, finding.Message); err != nil {
				return err
			}
		}
	}
	return nil
}

func WriteApply(w io.Writer, format string, result ApplyResult) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	if err := WriteAudit(w, format, result.Report); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "mutated=%d skipped=%d auto_fixable=%d\n", result.Mutated, result.Skipped, result.AutoFixable)
	return err
}

func WriteStorageCleanupAudit(w io.Writer, format string, report StorageCleanupReport) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	if _, err := fmt.Fprintf(w, "scope=%s/%s scanned=%d findings=%d\n", report.Organization, report.Project, report.Scanned, len(report.Findings)); err != nil {
		return err
	}
	for _, finding := range report.Findings {
		if _, err := fmt.Fprintf(w, "%s [%s] %s action=%s scope=%s\n", finding.NormalizedPath, finding.Kind, finding.Message, finding.RecommendedAction, finding.CleanupScope); err != nil {
			return err
		}
		for _, record := range finding.Records {
			if _, err := fmt.Fprintf(w, "  - %s storage=%s downloads=%d scope=%s\n", record.ObjectID, record.StorageStatus, record.DownloadCount, record.CleanupScope); err != nil {
				return err
			}
			for _, probe := range record.AccessProbes {
				if _, err := fmt.Fprintf(w, "    * %s storage=%s bucket=%s error=%s message=%s\n", probe.URL, probe.StorageStatus, probe.Bucket, probe.ErrorKind, probe.StorageMessage); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func WriteStorageCleanupApply(w io.Writer, format string, result StorageCleanupApplyResult) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	if err := WriteStorageCleanupAudit(w, format, result.Report); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "dry_run=%t deleted=%d skipped=%d repo_delete_paths=%d\n", result.DryRun, len(result.DeletedRecordIDs), len(result.Skipped), len(result.RepoDeletePaths)); err != nil {
		return err
	}
	for _, skipped := range result.Skipped {
		if _, err := fmt.Fprintf(w, "  - skipped %s %s: %s\n", skipped.Kind, skipped.ObjectID, skipped.Reason); err != nil {
			return err
		}
	}
	return nil
}
