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
    :depends-on ( :asdf
                  :cffi-grovel
                  :cl-base64
                  :cl-store
                  :cl-date-time-parser
                  :cl-fad
                  :cl-json
                  :closer-mop
                  :fare-memoization
                  :gzip-stream
                  :jonathan
                  :local-time
                  :manardb
                  :pcall
                  :pcall-queue
                  :salza2
                  :split-sequence
                  :thnappy
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
                               (:file "database" :depends-on ("package"))
                               (:file "db-manardb" :depends-on ("package" "database"))
                               (:file "flows" :depends-on ("package" "ctcl" "utils" "database"))
                               (:file "main" :depends-on ("package" "ctcl" "utils" "database" "flows"))
                               ))))
