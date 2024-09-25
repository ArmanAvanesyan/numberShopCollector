class UcomSelectors:
    PRODUCT = 'div.numbers_list.wrapper ul li'
    MOBILE_NUMBER = 'data-number'
    STATUS = 'div.number_block::attr(class)'
    NEXT_PAGE = 'li.pages-item-next a.next::attr(href)'


class TeamSelectors:
    PRODUCT = '.e-shop__mobile-list-item'
    MOBILE_NUMBER = '.e-shop__mobile-top-box ::text'
    NEXT_PAGE = '.paging__arrow--next::attr(href)'
