package operations

import (
	"context"
	"sort"

	"github.com/rclone/rclone/fs/hash"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
)

// dedupeDeleteIdentical deletes all but one of identical (by hash) copies
func findDupeIdentical(ctx context.Context, ht hash.Type, remote string, objs []fs.Object) (remainingObjs []fs.Object) {
	// See how many of these duplicates are identical
	byHash := make(map[string][]fs.Object, len(objs))
	for _, o := range objs {
		md5sum, err := o.Hash(ctx, ht)
		if err != nil || md5sum == "" {
			remainingObjs = append(remainingObjs, o)
		} else {
			byHash[md5sum] = append(byHash[md5sum], o)
		}
	}

	// Delete identical duplicates, filling remainingObjs with the ones remaining
	for md5sum, hashObjs := range byHash {
		remainingObjs = append(remainingObjs, hashObjs[0])
		if len(hashObjs) > 1 {
			fs.Logf(remote, "Find %d/%d identical duplicates (%v %q)", len(hashObjs)-1, len(hashObjs), ht, md5sum)
			for _, o := range hashObjs {
				fs.Logf(remote, "file: %s/%s", o.Fs().String(), o.String())
			}
		}
	}

	return remainingObjs
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

// dedupeMergeDuplicateDirs merges all the duplicate directories found
func dedupeMergeDuplicateDirs(ctx context.Context, f fs.Fs, duplicateDirs [][]fs.Directory) error {
	mergeDirs := f.Features().MergeDirs
	if mergeDirs == nil {
		return errors.Errorf("%v: can't merge directories", f)
	}
	dirCacheFlush := f.Features().DirCacheFlush
	if dirCacheFlush == nil {
		return errors.Errorf("%v: can't flush dir cache", f)
	}
	for _, dirs := range duplicateDirs {
		if !fs.Config.DryRun {
			fs.Infof(dirs[0], "Merging contents of duplicate directories")
			err := mergeDirs(ctx, dirs)
			if err != nil {
				err = fs.CountError(err)
				fs.Errorf(nil, "merge duplicate dirs: %v", err)
			}
		} else {
			fs.Infof(dirs[0], "NOT Merging contents of duplicate directories as --dry-run")
		}
	}
	dirCacheFlush()
	return nil
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

// Deduplicate interactively finds duplicate files and offers to
// delete all but one or rename them to be different. Only useful with
// Google Drive which can have duplicate file names.
func Duplicate(ctx context.Context, f fs.Fs) error {
	fs.Infof(f, "Looking for duplicates")

	// Find duplicate directories first and fix them
	duplicateDirs, err := dedupeFindDuplicateDirs(ctx, f)
	if err != nil {
		return err
	}
	if len(duplicateDirs) != 0 {
		err = dedupeMergeDuplicateDirs(ctx, f, duplicateDirs)
		if err != nil {
			return err
		}
	}

	// find a hash to use
	ht := f.Hashes().GetOne()

	// Now find duplicate files
	files := map[string][]fs.Object{}
	err = walk.ListR(ctx, f, "", true, fs.Config.MaxDepth, walk.ListObjects, func(entries fs.DirEntries) error {
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
			objs = findDupeIdentical(ctx, ht, remote, objs)
			//if len(objs) <= 1 {
			//	fs.Logf(remote, "All duplicates removed")
			//	continue
			//}

			//for _, o := range objs {
			//	fs.Logf(remote, "file: %s -- %s", o.String(), o.Fs().String())
			//}

		}
	}
	return nil
}
