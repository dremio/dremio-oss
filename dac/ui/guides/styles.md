# Styles

Just use CSS classes and PostCSS.

See EllipsedText for a simple example.

Pros:
- fast
- other components/pages can provide style additions / overrides for your component using the class name as an API
- web inspector support: e.g. change once and see all change on the fly

Cons:
- small chance of naming collision

Other Notes:
- You can use inline styles as you feel it's appropriate - it just shouldn't be the norm.
- Mark "private" class names with an underscore prefix.
- Try to avoid high-specificity selectors.
- Try to use and style semantic elements.
- No ids! (at least not for styles, you may need them for label relationships of the like)
