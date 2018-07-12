augroup javascript
    autocmd!

    nnoremap gd :TSDef<CR>

    let g:nvim_typescript#javascript_support = 1
    let g:jsx_ext_required = 0
    let g:UltiSnipsSnippetDirectories=["UltiSnips"]

    autocmd FileType javascript set tabstop=4|set shiftwidth=4|set expandtab
augroup END
