package operations

import (
	"context"
	"regexp"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
)

// Deduplicate interactively finds duplicate files and offers to
// delete all but one or rename them to be different. Only useful with
// Google Drive which can have duplicate file names.
func Panini(ctx context.Context, f fs.Fs) error {
	fs.Infof(f, "Looking for duplicates")

	// Now find duplicate files
	directories := map[string]fs.Directory{}
	files := map[string][]fs.Object{}

	var re = regexp.MustCompile(`\d{6,10}`)

	err := walk.ListR(ctx, f, "", true, 1, walk.ListObjects, func(entries fs.DirEntries) error {
		entries.ForDir(func(dir fs.Directory) {
			remote := dir.Remote()
			if dir != nil {
				if len(re.FindStringIndex(remote)) > 0 {
					directories[re.FindString(remote)] = dir
				}
			}
		})

		entries.ForObject(func(o fs.Object) {
			remote := o.Remote()
			if len(re.FindStringIndex(remote)) > 0 {
				files[re.FindString(remote)] = append(files[re.FindString(remote)], o)
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
				fs.Logf("%s dir: %s ", id, dir.Remote())
				if o.Fs().Root() != dir.Remote() {
					fs.Logf("%s new file: %s/%s ", id, o.Fs().Root(), o.String())
				} else {
					fs.Logf("%s old file: %s/%s ", id, o.Fs().Root(), o.String())
				}

				// if !fs.Config.DryRun {
				// 	if err := operations.MoveFile(ctx, f, f, o.String(), o.String()); err != nil {
				// 		fs.Logf(id, "deleted file failed, %s/%s  --- %s", o.Fs().Root(), o.String(), err.Error())
				// 		break
				// 	}
				// } else {
				// 	fs.Logf(id, "dry-run deleted file, %s/%s ", o.Fs().Root(), o.String())
				// }
			}
		}
	}
	return nil
}
