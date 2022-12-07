FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-google-workspace"]
COPY baton-google-workspace /