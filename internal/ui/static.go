package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

// web/dist holds the built frontend (Vite output). The placeholder index.html
// below is replaced by `npm run build`; the source lives in ui/web/.
//
//go:embed all:web/dist
var webFSRoot embed.FS

// webFS returns the embedded dist tree rooted at web/dist.
func webFS() fs.FS {
	sub, err := fs.Sub(webFSRoot, "web/dist")
	if err != nil {
		panic(err)
	}
	return sub
}

// staticHandler serves the embedded frontend with SPA fallback: unknown paths
// fall back to index.html so client-side routing keeps working on refresh.
func staticHandler() http.Handler {
	fsys := webFS()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFileFS(w, r, fsys, "index.html")
			return
		}
		http.FileServer(http.FS(fsys)).ServeHTTP(w, r)
	})
}
