package operations

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rclone/rclone/fs/operations"

	"github.com/rclone/rclone/fs/hash"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
)

// dedupeDeleteIdentical deletes all but one of identical (by hash) copies
func findDupeIdentical(ctx context.Context, ht hash.Type, remote string, objs []fs.Object) (deleteObjs []fs.Object) {
	// See how many of these duplicates are identical
	byHash := make(map[string][]fs.Object, len(objs))
	for _, o := range objs {
		if filepath.Ext(o.String()) == ".ass" || filepath.Ext(o.String()) == ".ssa" || filepath.Ext(o.String()) == ".srt" || filepath.Ext(o.String()) == ".nfo" {
			continue
		}
		if o.Size() == 0 {
			fs.Logf(remote, "ZeroSizeFile: %s/%s", o.Fs().Root(), o.String())
			deleteObjs = append(deleteObjs, o)
			continue
		}
		md5sum, err := o.Hash(ctx, ht)
		if err != nil || md5sum == "" {
			deleteObjs = append(deleteObjs, o)
		} else {
			byHash[md5sum] = append(byHash[md5sum], o)
		}
	}

	// Delete identical duplicates, filling remainingObjs with the ones remaining
	for _, hashObjs := range byHash {
		sortFileNameFirst(hashObjs)
		if len(hashObjs) > 1 {
			fs.Logf(remote, "--------------------------------------------------------")
			fs.Logf(remote, "KeepFile: %s/%s", hashObjs[0].Fs().Root(), hashObjs[0].String())
			for _, o := range hashObjs[1:] {
				fs.Logf(remote, "DeleteFile : %s/%s", o.Fs().Root(), o.String())
				deleteObjs = append(deleteObjs, o)
			}
		}
	}

	return deleteObjs
}

// dedupeFindDuplicateDirs scans f for duplicate directories
func dedupeFindDuplicateDirs(ctx context.Context, f fs.Fs) ([][]fs.Directory, error) {
	dirs := map[string][]fs.Directory{}
	err := walk.ListR(ctx, f, "", true, fs.Config.MaxDepth, walk.ListDirs, func(entries fs.DirEntries) error {
		entries.ForDir(func(d fs.Directory) {
			dirs[d.Remote()] = append(dirs[d.Remote()], d)
		})
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "find duplicate dirs")
	}
	// make sure parents are before children
	duplicateNames := []string{}
	for name, ds := range dirs {
		if len(ds) > 1 {
			duplicateNames = append(duplicateNames, name)
		}
	}
	sort.Strings(duplicateNames)
	duplicateDirs := [][]fs.Directory{}
	for _, name := range duplicateNames {
		duplicateDirs = append(duplicateDirs, dirs[name])
	}
	return duplicateDirs, nil
}

// sort oldest first
func sortOldestFirst(objs []fs.Object) {
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].ModTime(context.TODO()).Before(objs[j].ModTime(context.TODO()))
	})
}

// sort smallest first
func sortSmallestFirst(objs []fs.Object) {
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Size() < objs[j].Size()
	})
}

// sort smallest first
func sortFileNameFirst(objs []fs.Object) {
	sort.Slice(objs, func(i, j int) bool {
		return len(strings.Split(objs[i].String(), "/")[len(strings.Split(objs[i].String(), "/"))-1]) > len(strings.Split(objs[j].String(), "/")[len(strings.Split(objs[j].String(), "/"))-1])
	})
}

// Deduplicate interactively finds duplicate files and offers to
// delete all but one or rename them to be different. Only useful with
// Google Drive which can have duplicate file names.
func Duplicate(ctx context.Context, f fs.Fs) error {
	fs.Infof(f, "Looking for duplicates")

	// Find duplicate directories first and fix them
	//duplicateDirs, err := dedupeFindDuplicateDirs(ctx, f)
	//if err != nil {
	//	return err
	//}
	//if len(duplicateDirs) != 0 {
	//	err = dedupeMergeDuplicateDirs(ctx, f, duplicateDirs)
	//	if err != nil {
	//		return err
	//	}
	//}

	// find a hash to use
	ht := f.Hashes().GetOne()

	// Now find duplicate files
	files := map[string][]fs.Object{}
	err := walk.ListR(ctx, f, "", true, fs.Config.MaxDepth, walk.ListObjects, func(entries fs.DirEntries) error {
		entries.ForObject(func(o fs.Object) {
			remote := o.Remote()
			md5sum, err := o.Hash(ctx, ht)
			if err != nil {
				fs.Logf(remote, err.Error())
			}
			//fs.Logf(remote, "file %s -- %d", o.Fs().String(), o.Size())
			files[md5sum] = append(files[md5sum], o)
		})
		return nil
	})
	if err != nil {
		return err
	}

	for remote, objs := range files {
		if len(objs) > 1 {
			findDupeIdentical(ctx, ht, remote, objs)

			objs = findDupeIdentical(ctx, ht, remote, objs)

			// fs.Logf(remote, "===========================================================================")

			for _, o := range objs {
				if !fs.Config.DryRun {
					if err := operations.DeleteFile(ctx, o); err != nil {
						fs.Logf(remote, "deleted file failed, %s/%s  --- %s", o.Fs().Root(), o.String(), err.Error())
						break
					}
				} else {
					fs.Logf(remote, "dry-run deleted file, %s/%s ", o.Fs().Root(), o.String())
				}
			}

		}
	}
	return nil
}
