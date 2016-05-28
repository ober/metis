#-asdf3 (error "metis requires ASDF 3")

(asdf:defsystem :metis
  :name "Common Lisp Metis Library"
  :description "A Common Lisp client library for the Metis"
  :version "0.0.1"
  :license "MIT"
  :class :package-inferred-system
  :defsystem-depends-on (:asdf-package-system)
  :depends-on (:metis/pkgdcl))

