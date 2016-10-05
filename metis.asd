#-asdf3 (error "metis requires ASDF 3")

(defsystem #:metis
  :name "metis"
  :description "A Common Lisp client library for the Metis"
  :version "0.0.1"
  :license "MIT"
  :class :package-inferred-system
  :defsystem-depends-on (:asdf-package-system)
  :depends-on (:quicklisp
	       :fare-memoization
	       :cl-fad
	       :gzip-stream
	       :cl-json
	       :com.gigamonkeys.json
	       :pcall
	       :postmodern
	       :split-sequence
	       :cl-date-time-parser
	       :local-time)
  :components (
	       (:file "package")
	       (:file "ctcl" :depends-on ("package" "utils" "database"))
	       (:file "utils" :depends-on ("package" "database"))
	       (:file "version" :depends-on ("package"))
	       (:file "bench" :depends-on ("package" "ctcl"))
	       (:file "database" :depends-on ("package"))
	       (:file "flows" :depends-on ("package" "ctcl" "utils" "database"))
	       (:file "main" :depends-on ("package" "ctcl" "utils" "database" "flows"))
	       (:file "pkgdcl" :depends-on ("package" "ctcl" "utils" "database"))
	       ))
