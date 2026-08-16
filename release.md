## Releasing

1. `meteor whoami` — check you are logged in; make sure there is no
   local `node_modules` (Meteor ships the package directory as-is)
2. bump `version:` in `package.js`
3. `./test.sh --full`
4. `meteor publish --release METEOR@2.15` — irreversible, a published version
   cannot be removed. Regenerates `.versions`.
5. confirm it landed: `grep danmacko:publish-relations .versions`
6. `git add package.js .versions && git commit -m "chore: release X.Y.Z"`
7. `git tag -a vX.Y.Z -m vX.Y.Z && git push origin master --tags`
8. write the GitHub release notes — the README header points readers there
   as the changelog
