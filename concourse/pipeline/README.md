### Generate PR pipeline with ytt

```
ytt --data-values-file res_def.yml \
        -f base.lib.yml \
        -f job_def.lib.yml \
        -f pr.yml
```
