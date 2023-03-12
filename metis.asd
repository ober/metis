#-asdf3 (error "metis requires ASDF 3")

(defsystem #:metis
    :name "metis"
    :description "A Common Lisp client library for the Metis"
    :version "0.0.1"
    :license "MIT"
    :class :package-inferred-system
    :build-operation program-op
    :build-pathname "metis"
    :defsystem-depends-on (:asdf-package-system)
    :entry-point "metis:main"
    :depends-on (
                 :alexandria
                 :asdf
                 :cffi-grovel
                 :cl-date-time-parser
                 :cl-fad
                 :cl-json
                 :closer-mop
                 :fare-memoization
                 :for
                 :gzip-stream
                 :local-time
                 :manardb
                 :pcall
                 :pcall-queue
                 :salza2
                 :shasht
                 :split-sequence
                 :cl-ssdb
                 :trivial-garbage
                 :uiop
                 :usocket
                 :zs3
                 )
    :components ((:module src :serial t
                  :components (
                               (:file "package")
                               (:file "ctcl")
                               (:file "utils")
                               (:file "version" :depends-on ("package"))
                               (:file "bench")
                               (:file "database" :depends-on ("package"))
                               ;;(:file "db-manardb" :depends-on ("package" "database"))
                               (:file "db-ssdb" :depends-on ("package" "database"))
                               (:file "flows" :depends-on ("package" "ctcl" "utils" "database"))
                               (:file "main" :depends-on ("package" "ctcl" "utils" "database" "flows"))
                               ))))
