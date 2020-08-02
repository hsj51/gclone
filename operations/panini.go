package operations

import (
	"context"
	"fmt"
	"regexp"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
)

// Deduplicate interactively finds duplicate files and offers to
// delete all but one or rename them to be different. Only useful with
// Google Drive which can have duplicate file names.
func Panini(ctx context.Context, f fs.Fs) error {
	fs.Infof(f, "Panini")

	// Now find duplicate files
	directories := map[string]fs.Directory{}
	files := map[string][]fs.Object{}

	var re = regexp.MustCompile(`(\d{6,10})`)

	err := walk.ListR(ctx, f, "", true, fs.Config.MaxDepth, walk.ListDirs, func(entries fs.DirEntries) error {
		entries.ForDir(func(dir fs.Directory) {
			remote := dir.Remote()
			if dir != nil {
				if len(re.FindStringIndex(remote)) > 0 {
					matches := re.FindAllStringSubmatch(remote, -1)
					// fs.Infof("find directory", matches[len(matches)-1][1])
					directories[matches[len(matches)-1][1]] = dir
				}
			}
		})

		return nil
	})

	err = walk.ListR(ctx, f, "", true, 1, walk.ListObjects, func(entries fs.DirEntries) error {
		entries.ForObject(func(o fs.Object) {
			remote := o.Remote()
			if len(re.FindStringIndex(remote)) > 0 {
				matches := re.FindAllStringSubmatch(remote, -1)
				// fs.Infof("find file", matches[len(matches)-1][1])
				files[matches[len(matches)-1][1]] = append(files[matches[len(matches)-1][1]], o)
			}
		})
		return nil
	})

	if err != nil {
		return err
	}

	for id, dir := range directories {
		if objs, ok := files[id]; ok {
			for _, o := range objs {
				fs.Logf(id, "===========================================================================")
				// fs.Logf(id, "dir: %s  file: %s ", o.Fs().Root(), o.String())
				// fs.Logf(id, "dir: %s ", dir.Remote())
				if o.Fs().Root() != dir.Remote() {
					fs.Logf(id, "move from %s/%s to %s/%s ", o.Fs().Root(), o.String(), dir.Remote(), o.String())
				} else {
					fs.Logf(id, "exists file: %s/%s ", o.Fs().Root(), o.String())
				}

				if !fs.Config.DryRun {
					dest := fmt.Sprintf("%s/%s", dir.Remote(), o.String())
					src := fmt.Sprintf("%s/%s", o.Fs().Root(), o.String())
					if err := operations.MoveFile(ctx, f, f, dest, src); err != nil {
						fs.Logf(id, "move file failed, %s/%s  --- %s", o.Fs().Root(), o.String(), err.Error())
						break
					}
				} else {
					fs.Logf(id, "dry-run move file, from %s/%s to %s/%s ", o.Fs().Root(), o.String(), dir.Remote(), o.String())
				}
			}
		}
	}
	return nil
}
